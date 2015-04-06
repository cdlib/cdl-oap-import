#!/usr/bin/env ruby

# This script groups publications together using title, author and identifiers.
# The resulting de-duped "OA Publications" will be used for OAP identifier assignment and then pushing
# into Symplectic Elements via the API.
#
# TODO: Associate these records, when we have an Elements ID, with the Elements item.
#
# The code is very much a work in progress.

# System libraries
require 'cgi'
require 'date'
require 'digest/sha1'
require 'ezid'
require 'fileutils'
require 'net/http'
require 'netrc'
require 'nokogiri'
require 'ostruct'
require 'set'
require 'sqlite3'
require 'thread'
require 'zlib'

# Flush stdout after each write
STDOUT.sync = true

# Special args
$forceMode = ARGV.delete('--force')
$reportMode = ARGV.delete('--report')

if ARGV.include? '--only'
  pos = ARGV.index '--only'
  ARGV.delete_at pos
  $onlyCampus = ARGV[pos]
  ARGV.delete_at pos
end

# Global variables
$allItems = []
$docKeyToItems = Hash.new{|h, k| h[k] = Array.new }
$titleCount = Hash.new{|h, k| h[k] = 0 }
$authKeys = Hash.new{|h, k| h[k] = calcAuthorKeys(k) }
$pubs = []
$emailToElementsUser = Hash.new{|h, k| h[k] = lookupElementsUser(k) }
$credentials = nil
$db = SQLite3::Database.new("oap.db")
$db.busy_timeout = 30000
$dbMutex = Mutex.new
$transLog = nil
$toPost = nil
$reportFile = nil
$mintQueue = SizedQueue.new(100)
$importQueue = SizedQueue.new(100)

ALL_CAMPUSES = ['eschol', 'ucla', 'uci', 'ucsf']

# Elements type IDs
$typeIds = { 2 => 'book',
             3 => 'chapter',
             4 => 'conference',
             5 => 'article' }

# Common English stop-words
$stopwords = Set.new(("a an the of and to in that was his he it with is for as had you not be her on at by which have or " +
                      "from this him but all she they were my are me one their so an said them we who would been will no when").split)

# Structure for holding information on an item
Item = Struct.new(:typeId, :title, :docKey, :authors, :date, :ids, :journal, :volume, :issue, :otherInfo) {
  def campusIDs
    ids.select { |scheme, text| isCampusID(scheme) }
  end
}

# Less common item info (only for books, conferences, etc.)
OtherItemInfo = Struct.new(:abstract, :editors, :publisher, :placeOfPublication, :pagination, :nameOfConference, :parentTitle)

# Structure for holding a group of duplicate items
OAPub = Struct.new(:items, :userEmails, :userPropIds)

# Make puts thread-safe
$stdoutMutex = Mutex.new
def puts(*args)
  $stdoutMutex.synchronize {
    super(*args)
  }
end

###################################################################################################
# Determine the Elements API instance to connect to, based on the host name
$hostname = `/bin/hostname`.strip
$elementsUI = case $hostname
  when 'submit-stg', 'submit-dev'; 'https://qa-oapolicy.universityofcalifornia.edu'
  when 'cdl-submit-p01'; 'https://oapolicy.universityofcalifornia.edu'
  else 'http://unknown-host'
end
$elementsAPI = "#{$elementsUI}:8002/elements-secure-api"

# We need to identify recurring series items and avoid grouping them. Best way seems to be just by title.
seriesTitles = [
  # Fairly generic stuff
  "About the Contributors",
  "Acknowledg(e?)ments",
  "Advertisement(s?)",
  "Author's Biographies",
  "(Back|End|Front) (Cover|Matter)",
  "(Books )?noted with interest",
  "Books Received",
  "Brief Notes on Recent Publications",
  "Call for Papers",
  "Conference Program",
  "Contents",
  "Contributors",
  "Cover",
  "(Editor's|Editors|Editors'|President's) (Introduction|Message|Note|Page)",
  "Editorial",
  "Editorial Notes",
  "Foreword",
  "(Forward |Reprise )?Editor's Note",
  "Full Issue",
  "Introduction",
  "Job announcements",
  "Letter from the Editors",
  "Legislative Update",
  "Masthead",
  "New Titles",
  "Preface",
  "Publications Received",
  "Review",
  "(The )?Table of Contents",
  "Thanks to reviewers",
  "Untitled",
  "Upcoming events",
  # Stuff that's likely specific to eScholarship
  "Beyond the Frontier II",
  "Conceiving a Courtyard",
  "Environmental Information Sources",
  "Índice",
  "Lider/ Poems",
  "Summary of the Research Progress Meeting",
  "Three pieces",
  "Two Poems",
  "UCLA French Department Dissertation Abstracts",
  "UCLA French Department Publications and Dissertations"
]
$seriesTitlesPat = Regexp.new("^(#{seriesTitles.join('|').downcase})$")

# Transliteration tables -- a cheesy but easy way to remove accents without requiring a Unicode gem
$transFrom = "ÀÁÂÃÄÅàáâãäåĀāĂăĄąÇçĆćĈĉĊċČčÐðĎďĐđÈÉÊËèéêëĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħÌÍÎÏìíîïĨĩĪīĬĭĮįİıĴĵĶķĸĹĺĻļĽ" +
             "ľĿŀŁłÑñŃńŅņŇňŉŊŋÒÓÔÕÖØòóôõöøŌōŎŏŐőŔŕŖŗŘřŚśŜŝŞşŠšſŢţŤťŦŧÙÚÛÜùúûüŨũŪūŬŭŮůŰűŲųŴŵÝýÿŶŷŸŹźŻżŽž"
$transTo   = "AAAAAAaaaaaaAaAaAaCcCcCcCcCcDdDdDdEEEEeeeeEeEeEeEeEeGgGgGgGgHhHhIIIIiiiiIiIiIiIiIiJjKkkLlLlL" +
             "lLlLlNnNnNnNnnNnOOOOOOooooooOoOoOoRrRrRrSsSsSsSssTtTtTtUUUUuuuuUuUuUuUuUuUuWwYyyYyYZzZzZz"

###################################################################################################
# Monkey patches to make Nokogiri even more elegant
class Nokogiri::XML::Node
  def text_at(xpath)
    at(xpath) ? at(xpath).text : nil
  end
end

###################################################################################################
# Remove accents from a string
def transliterate(str)
  str.tr($transFrom, $transTo)
end

###################################################################################################
# Convert to lower case, remove HTML-like elements, strip out weird characters, normalize spaces.
def normalize(str)
  str or return ''
  # NO: tmp = transliterate(str)  # This is dangerous: strips out accents in titles and author names!
  tmp = str.gsub(/&lt[;,]/, '<').gsub(/&gt[;,]/, '>').gsub(/&#?\w+[;,]/, '')
  tmp = tmp.gsub(/<[^>]+>/, ' ').gsub(/\\n/, ' ')
  tmp = tmp.gsub(/[|]/, '')
  tmp = tmp.gsub(/\s\s+/,' ').strip
  return tmp
end

###################################################################################################
# Special normalization for identifiers
def normalizeIdentifier(str)
  str or return ''
  tmp = str.downcase.strip
  tmp = tmp.sub(/^https?:\/\/dx.doi.org\//, '').sub(/^(doi(\.org)?|pmid|pmcid):/, '').sub(/\.+$/, '')
  tmp = tmp.sub(/^\[(.*)\]$/, '\1').sub(/^"(.*)"$/, '\1').sub(/^\[(.*)\]$/, '\1').sub(/^"(.*)"$/, '\1')
  return tmp
end

###################################################################################################
# Special normalization for ERC metadata
def normalizeERC(str)
  return transliterate(normalize(str)).encode('ascii', {:replace => '.'})
end

###################################################################################################
# Title-specific normalization
def filterTitle(title)
  # Break it into words, and remove the stop words.
  transliterate(normalize(title)).downcase.gsub(/[^a-z0-9 ]/, '').split.select { |w| !$stopwords.include?(w) }
end

###################################################################################################
def calcAuthorKeys(item)
  Set.new(item.authors.map { |auth|
    transliterate(normalize(auth)).downcase.gsub(/[^a-z]/,'')[0,4]
  })
end  

###################################################################################################
# Determine if the title is a likely series item
def isSeriesTitle(title)
  $titleCount[title] == 1 and return false
  ft = transliterate(title).downcase.gsub(/^[\[\(]|[\)\]]$/, '').gsub(/\s\s+/, ' ').gsub('’', '\'').strip()
  return $seriesTitlesPat.match(ft)
end

###################################################################################################
def isCampusID(scheme)
  return scheme =~ /^c-/
end

###################################################################################################
# See if the candidate is compatible with the existing items in the group
def isCompatible(items, cand)
  candAuthKeys = $authKeys[cand]
  return true if candAuthKeys.empty?
  ok = true
  ids = {}

  # Make sure the candidate has same type-id
  items.each { |item|
    if item.typeId != cand.typeId
      #puts "Type-id mismatch for scheme #{scheme.inspect}: #{cand.typeId.inspect} vs #{item.typeId.inspect}"
      ok = false
    end
  }

  # Make sure the candidate overlaps at least one author of every pub in the set
  items.each { |item|
    itemAuthKeys = $authKeys[item]
    next if itemAuthKeys.empty?
    overlap = itemAuthKeys & candAuthKeys
    if overlap.empty?
      #puts "No overlap: #{itemAuthKeys.to_a.join(',')} vs. #{candAuthKeys.to_a.join(',')}"
      ok = false
    end
    item.ids.each { |scheme, text|
      ids[scheme] = text
    }
  }

  # Make sure the candidate has no conflicting IDs
  cand.ids.each { |scheme, text|
    next if isCampusID(scheme)   # we know that campus IDs overlap each other, and that's expected
    if ids.include?(scheme) && ids[scheme] != text
      #puts "ID mismatch for scheme #{scheme.inspect}: #{text.inspect} vs #{ids[scheme].inspect}"
      ok = false
    end
  }

  # All done.
  return ok
end

###################################################################################################
def parseElemNativeRecord(native, typeId)

  # Title parsing and doc key generation
  title = normalize(native.text_at("field[@name='title']/text"))
  docKey = filterTitle(title).join(' ')

  # Author parsing
  authors = native.xpath("field[@name='authors']/people/person").map { |person|
    lname = normalize(person.text_at('last-name'))
    initials = normalize(person.text_at('initials'))
    email = normalize(person.text_at('email-address'))
    "#{lname}, #{initials}|#{email}"
  }
  
  # Date parsing
  dateField = native.at("field[@name='publication-date']/date")
  if dateField
    year = dateField.at("year") ? dateField.at("year").text : '0'
    month = dateField.at("month") ? dateField.at("month").text : '0'
    day = dateField.at("day") ? dateField.at("day").text : '0'
    date = "#{year.rjust(4,'0')}-#{month.rjust(2,'0')}-#{day.rjust(2,'0')}"
  else
    date = nil
  end

  # Identifier parsing
  ids = []
  ALL_CAMPUSES.each { |campus|
    tmp = native.text_at("field[@name='c-#{campus}-id']/text")
    tmp and ids << ["c-#{campus}-id", normalizeIdentifier(tmp)]
  }

  tmp = native.text_at("doi/text")
  tmp or tmp = native.text_at("field[@name='doi']/text")
  tmp and ids << ['doi', normalizeIdentifier(tmp)]

  native.xpath("field[@name='external-identifiers']/identifiers/identifier").each { |ident|
    scheme = ident['scheme'].downcase.strip
    text = normalizeIdentifier(ident.text)
    ids << [scheme, text]
  }

  # Journal/vol/iss
  journal = native.text_at("field[@name='journal']/text")
  volume  = native.text_at("field[@name='volume']/text")
  issue   = native.text_at("field[@name='issue']/text")

  # Other info (not present for most items, since most are journal articles)
  other = OtherItemInfo.new
  other.abstract = native.text_at("field[@name='abstract']/text")
  other.publisher = native.text_at("field[@name='publisher']/text")
  other.placeOfPublication = native.text_at("field[@name='place-of-publication']/text")
  other.nameOfConference = native.text_at("field[@name='name-of-conference']/text")
  other.parentTitle = native.text_at("field[@name='parent-title']/text")

  if native.text_at("field[@name='editors']//person/last-name") && native.text_at("field[@name='editors']//person/last-name").length > 0
    other.editors = native.xpath("field[@name='editors']//person").map { |person|
      lname = normalize(person.text_at('last-name'))
      initials = normalize(person.text_at('initials'))
      email = normalize(person.text_at('email-address'))
      "#{lname}, #{initials}|#{email}"
    }
  end

  if native.at("field[@pagination]/text")
    str = native.text_at("field[@pagination]/text").strip
    if str =~ /(\d+)\s*-\s*(\d+)/
      firstPage = $1
      lastPage = $2
      if lastPage.length < firstPage.length   # handle "213-23"
        lastPage = firstPage[0, firstPage.length - lastPage.length] + lastPage
      end
      other.pagination = [firstPage, lastPage]
    end
  end

  # Bundle up the result
  return Item.new(typeId, title, docKey, authors, date, ids, journal, volume, issue, (other.select{|v| v}.empty?) ? nil : other)
end

###################################################################################################
def authMergeKey(name)
  name.sub(/, (\w)\w$/, ', \1').downcase
end

###################################################################################################
# UCSF may supply apparent dupes to us, but each record has one of the authors' email addresses
# filled in. This code checks that the authors are the same, and fills in the missing email.
def mergeAuthorInfo(dst, src)

  # We will do 8-character matching, but fall back to 4-character if we can't do that.
  dst4, dst8 = {}, {}
  dst.authors.each_with_index { |auth, n| 
    dstName, dstEmail = auth.split('|')
    next if dstEmail
    key = authMergeKey(dstName)
    dst8[key[0,8]] = n 
    dst4[key[0,4]] = n
  }

  campusID = dst.campusIDs[0][1]
  origStr = "\n  " + dst.authors.map{|auth| auth.split('|')[0]}.sort.join(";\n  ")

  found = false
  srcWithEmail = nil
  src.authors.each { |srcAuth|
    srcName, srcEmail = srcAuth.split('|')
    next unless srcEmail
    srcWithEmail = srcAuth
    key = authMergeKey(srcName)[0,8]
    if dst8.include? key
      dst.authors[dst8[key]] = srcAuth
      found = 8
    end
  }
  if !found
    src.authors.each { |srcAuth|
      srcName, srcEmail = srcAuth.split('|')
      next unless srcEmail
      key = authMergeKey(srcName)[0,4]
      if dst4.include? key
        dst.authors[dst4[key]] = srcAuth
        found = 4
      end
    }
  end

  if !srcWithEmail
    # don't care, no email to match anyway
  elsif !found
    puts "Warning: In campus pub #{campusID}, unable to match #{srcWithEmail.inspect} to authors: #{origStr}"
  elsif found == 4
    puts "Note: In campus pub #{campusID}, less-happy match #{srcWithEmail.inspect} to authors: #{origStr}"
  else
    #puts "Matched #{srcWithEmail.inspect} to authors #{origStr} to yield #{dst.authors.map{|auth| auth.split('|')[0]}.join('; ')}"
  end
end

###################################################################################################
def iterateRecords(filename)
  io = (filename =~ /\.gz$/) ? Zlib::GzipReader.open(filename) : File.open(filename)
  buf = []
  io.each { |line|
    if line =~ /<import-record /
      buf = [line]
    elsif line =~ /<\/import-record/
      buf << line
      str = buf.join("")
      doc = Nokogiri::XML(str, nil, 'utf-8')
      doc.remove_namespaces!
      yield doc.root
    else
      buf << line
    end
  }
  io.close
end

###################################################################################################
def readItems(filename)
  FileUtils::mkdir_p('cache')
  cacheFile = "cache/#{filename}.cache"
  if File.exists?(cacheFile) && File.mtime(cacheFile) > File.mtime(filename)
    puts "Reading cache for #{filename}."
    Zlib::GzipReader.open(cacheFile) { |io| return Marshal.load(io) }
  else
    items = []
    campusIdToItem = {}
    puts "Reading and parsing #{filename}."
    nParsed = 0
    nDupes = 0
    iterateRecords(filename) { |record|
      record.name == 'import-record' or raise("Unknown record type '#{record.name}'")
      typeId = record.attr('type-id').to_i
      $typeIds[typeId] or raise("Unknown type-id #{typeId}")
      item = parseElemNativeRecord(record.at('native'), typeId)

      # A few records have no title. We can't do anything with them.
      if item.title == nil || item.title.length == 0
        idStr = item.ids.map { |kind, id| "#{kind}::#{id}" }.join(', ')
        puts "Warning: item with empty title: #{idStr} ... skipped."
        next
      end

      # Identifier parsing
      campusId = item.campusIDs[0]
      campusId or raise("No campus ID found: #{record}")

      # Bundle up the result (or in the case of a dupe merge the author info)
      if campusIdToItem.include? campusId
        nDupes += 1
        mergeAuthorInfo(campusIdToItem[campusId], item)
        next
     else
        campusIdToItem[campusId] = item
        items << item
        #puts item
      end

      # Give feedback every once in a while.
      nParsed += 1
      if (nParsed % 1000) == 0
        puts "...#{nParsed} parsed (#{nDupes} dupes merged)."
        # TODO: For debugging speed, stop after 10000 records. For real production run, take this out!
        #break if nParsed == 10000
      end      
    }
    puts "...#{nParsed} parsed (#{nDupes} dupes merged)."

    puts "Writing cache file."
    Zlib::GzipWriter.open(cacheFile) { |io| Marshal.dump(items, io) }
    return items
  end
end

###################################################################################################
def addItems(items)
  $allItems += items
  items.each { |item|
    $titleCount[item.title] += 1
    $docKeyToItems[item.docKey] << item
  }
end

###################################################################################################
def db_get_first_value(stmt, *more)
  $dbMutex.synchronize {
    return $db.get_first_value(stmt, *more)
  }
end

###################################################################################################
def db_execute(stmt, *more)
  $dbMutex.synchronize {
    return $db.execute(stmt, *more)
  }
end

###################################################################################################
def lookupElementsUser(email)
  return db_get_first_value("SELECT proprietary_id FROM emails WHERE email = ?", email)
end

###################################################################################################
# For items with a matching title key, group together by overlapping authors to form the OA Pubs.
def groupItems()
  $docKeyToItems.sort.each { |docKey, items|

    # If there are 5 or more dates involved, this is probably a series thing.
    numDates = items.map{ |info| info.date }.uniq.length

    # Singleton cases.
    if items.length == 1 || numDates >= 5 || isSeriesTitle(items[0].title)
      if numDates >= 5
        if !isSeriesTitle(items[0].title)
          #puts "Probable series (#{numDates} dates): #{items[0].title}"
        else
          #puts "Series would have been covered by date-count #{numDates}: #{items[0].title}"
        end
      end
      
      # Singletons: make a separate OAPub for each item
      items.each { |item| $pubs << OAPub.new([item]) }
      next
    end

    # We know the docs all share the same title key. Group those that have compatible authors,
    # ids, etc.
    while !items.empty?

      # Get the first item
      item1 = items.shift
      pub = OAPub.new([item1])

      # Match it up to every other item that's compatible
      items.dup.each { |item2|
        if isCompatible(pub.items, item2)
          items.delete(item2)
          pub.items << item2
        end
      }

      # Done with this grouped publication
      $pubs << pub
    end
  }

end

###################################################################################################
# Mint a new OAP identifier
def mintOAPID(metadata)
  resp = $ezidSession.mint(metadata)
  resp.respond_to?(:errored?) and resp.errored? and raise("Error minting ark: #{resp.response}")
  return resp.identifier
end

###################################################################################################
def makeItemStr(item)
  title = normalize(item.title)
  if item.date
    date = item.date.gsub("-01", "-1").gsub("-00", "-")  # penalize less-exact dates
  else
    date = "nil"
  end
  authors = item.authors.map { |auth| auth.split("|")[0] }.join(';')
  str = "#{title}|#{date}|#{authors}|#{item.journal}|#{item.volume}|#{item.issue}"
  return str
end
def isBetterItem(item1, item2)
  return makeItemStr(item1).length > makeItemStr(item2).length
end

###################################################################################################
def peopleToXML(xml, people)
  xml.people {
    people.each { |auth|
      xml.person {
        name, email = auth.split("|")
        last, initials = name.split(", ")
        xml.send('last-name', last)
        xml.initials(initials)
        email and email.strip != '' and xml.send('email-address', email)
      }
    }
  }
end

###################################################################################################
def makeRecordToPut(item, dedupedIds)
  Nokogiri::XML::Builder.new { |xml|
    xml.send('import-record', 'xmlns' => "http://www.symplectic.co.uk/publications/api", 
            'type-id' => item.typeId) { # 2 = book, 3 = chapter, 4 = conference, 5 = journal-article
      xml.native {

        # Title, author, journal/vol/iss, and date all taken from a single item chosen as the "best"
        xml.field(name: 'title') { xml.text_ item.title }
        xml.field(name: 'authors') { peopleToXML(xml, item.authors) }
        item.journal and xml.field(name: 'journal') { xml.text_ item.journal }
        item.volume and xml.field(name: 'volume') { xml.text_ item.volume }
        item.issue and xml.field(name: 'issue') { xml.text_ item.issue }
        if item.date
          xml.field(name: 'publication-date') {
            xml.date {
              item.date =~ /(\d\d\d\d)-0*(\d+)-0*(\d+)/
              year, month, day = $1, $2, $3
              year != 0 and xml.year year
              month != '0' and xml.month month
              day != '0' and xml.day day
            }
          }
        end

        # Identifiers from all items, but de-duped
        idsByScheme = Hash.new{|h, k| h[k] = Array.new }
        extIds = {}
        dedupedIds.each { |scheme, id| 
          idsByScheme[scheme] << normalizeIdentifier(id) 
        }
        idsByScheme.each { |scheme, ids|
          if scheme == 'doi'
            ids.length == 1 or raise("cannot have #{ids.length} DOIs in a record")
            xml.field(name: 'doi') { xml.text_ idsByScheme['doi'][0] }
          elsif isCampusID(scheme)
            xml.field(name: scheme) { xml.text_ ids.join(', ') }
          else
            ids.length == 1 or raise("cannot have #{ids.length} #{scheme} IDs in a record")
            extIds[scheme] = ids
          end
        }
        if !extIds.empty?
          xml.field(name: 'external-identifiers') {
            xml.identifiers {
              extIds.each { |scheme, ids|
                scheme == 'pmid' and scheme = 'pubmed'   # map pmid -> pubmed
                xml.identifier(scheme: scheme) { xml.text ids[0] }
              }
            }
          }
        end

        # Less common info (e.g. for books, conferences)
        other = item.otherInfo
        if other
          other.abstract and xml.field(name: 'abstract') { xml.text_ other.abstract }
          other.editors and xml.field(name: 'editors') { peopleToXML(xml, other.editors) }
          other.publisher and xml.field(name: 'publisher') { xml.text_ other.publisher }
          other.placeOfPublication and xml.field(name: 'place-of-publication') { xml.text_ other.placeOfPublication }
          other.nameOfConference and xml.field(name: 'name-of-conference') { xml.text_ other.nameOfConference }
          other.parentTitle and xml.field(name: 'parent-title') { xml.text_ other.parentTitle }
          if other.pagination
            xml.field(name: 'pagination') {
              xml.pagination {
                xml.send('begin-page', other.pagination[0])
                xml.send('end-page', other.pagination[1])
              }
            }
          end
        end
      }
    }
  }.to_xml
end

###################################################################################################
# Do the work of putting a record into Elements and recording the result.
# Returns: isJoinedRecord, isElemCompat, elemItem
def putRecord(pub, oapID, toPut)

  # Figure out the URL to send it to
  uri = URI("#{$elementsAPI}/publication/records/c-inst-1/#{CGI.escape(oapID)}")

  # Log what we're about to put.
  $transLog.write("\n---------------------------------------------------------------\n")
  $transLog.write("\nPUT #{uri}\n\n")
  $transLog.write(toPut)
  $transLog.flush

  # And put it
  req = Net::HTTP::Put.new(uri)
  req['Content-Type'] = 'text/xml'
  req.basic_auth $apiCred[0], $apiCred[1]
  req.body = toPut
  (1..10).each { |tryNumber|
    puts "  Putting record."
    res = Net::HTTP.start(uri.hostname, uri.port, :use_ssl => uri.scheme == 'https') { |http|
      http.request req
    }

    # Log the response
    $transLog.write("\nResponse:\n")
    $transLog.write("#{res}\n")
    $transLog.write("#{res.body.start_with?('<?xml') ? Nokogiri::XML(res.body, &:noblanks).to_xml : res.body}\n")

    # HTTPConflict and HTTPGatewayTimeOut happen occasionally, and are likely transitory
    if res.is_a?(Net::HTTPConflict) || res.is_a?(Net::HTTPGatewayTimeOut)
      puts "  Note: failed due to #{res} (likely a transitory concurrency issue)."
      if tryNumber < 20
        puts "  Will retry after a 30-second pause."
        sleep 30
        next
      else
        puts "  Out of retries. Aborting."
      end  
    end

    # Fail if the PUT failed
    res.is_a?(Net::HTTPSuccess) or raise("Error: put failed: #{res}")

    # Parse the result and record the associated Elements publication ID
    putResult = Nokogiri::XML(res.body, &:noblanks)
    putResult.remove_namespaces!
    pubID = putResult.at("entry/object[@category='publication']")['id']
    db_execute("INSERT OR REPLACE INTO pubs (pub_id, oap_id) VALUES (?, ?)", [pubID, oapID])

    # We want to know if Elements created a new record or joined to an existing one. We can tell
    # by checking if there are any other sources.
    obj = putResult.at("entry/object[@category='publication']")
    otherRecords = obj.xpath("records/record[@format='native']").select { |el| el['source-name'] != 'c-inst-1' }
    sources = otherRecords.map { |el| el['source-name'] }
    isJoinedRecord = !sources.empty?

    # Also we're curious if the joined record would meet our joining criteria. For that we need
    # to parse the metadata from the first record.
    isElemCompat = false
    elemItem = nil
    if isJoinedRecord
      # Pick scopus and crossref over other kinds of records, if available
      native = obj.at("records/record[@format='native'][source-name='scopus']")
      native or native = obj.at("records/record[@format='native'][source-name='crossref']")
      native or native = otherRecords[0]
      elemItem = parseElemNativeRecord(native.at('native'), obj.attr('type-id').to_i)
      isElemCompat = isCompatible(pub.items, elemItem)
    end

    # And we're done.
    return isJoinedRecord, isElemCompat, elemItem
  }

end

###################################################################################################
# Add a relationship between a publication and a user in Elements
def postRelationship(oapID, userPropID)
  toPost = Nokogiri::XML::Builder.new { |xml|
    xml.send('import-relationship', 'xmlns' => "http://www.symplectic.co.uk/publications/api") {
      xml.send('from-object', "publication(source-c-inst-1,pid-#{oapID})")
      xml.send('to-object', "user(pid-#{userPropID})")
      xml.send('type-name', "publication-user-authorship")
    }
  }.to_xml
  uri = URI("#{$elementsAPI}/relationships")

  # Log what we're about to POST.
  $transLog.write("\n---------------------------------------------------------------\n")
  $transLog.write("\nPOST #{uri}\n\n")
  $transLog.write(toPost)

  # And put it
  (1..10).each { |tryNumber|

    puts "  Posting relationship for user ID #{userPropID}."
    req = Net::HTTP::Post.new(uri)
    req['Content-Type'] = 'text/xml'
    req.basic_auth $apiCred[0], $apiCred[1]
    req.body = toPost
    res = Net::HTTP.start(uri.hostname, uri.port, :use_ssl => uri.scheme == 'https') { |http|
      http.request req
    }

    # Log the response
    $transLog.write("\nResponse:\n")
    $transLog.write("#{res}\n")
    $transLog.write("#{res.body.start_with?('<?xml') ? Nokogiri::XML(res.body, &:noblanks).to_xml : res.body}\n")

    # HTTPConflict and HTTPGatewayTimeOut happen occasionally, and are likely transitory
    if res.is_a?(Net::HTTPConflict) || res.is_a?(Net::HTTPGatewayTimeOut)
      puts "  Note: failed due to #{res} (likely a transitory concurrency issue)."
      if tryNumber < 20
        puts "  Will retry after a 30-second pause."
        sleep 30
        next
      else
        puts "  Out of retries. Aborting."
      end  
    end

    # Fail if the POST failed.
    res.is_a?(Net::HTTPSuccess) or raise("Error: post failed: #{res}")

    # Otherwise, we're done.
    return
  }
end

###################################################################################################
def printPub(postNum, pub, oapID)
    puts
    puts "Post \##{postNum} of #{$toPost.size}:"
    pub.items.each { |item|
      idStr = item.ids.map { |kind, id| "#{kind}::#{id}" }.join(', ')
      puts "  #{item.title} [#{$typeIds[item.typeId]}]"
      puts "      #{item.date ? item.date : '<no date>'}     #{item.authors.join('; ').gsub('|;', ';')}"
      puts "      #{idStr}"
    }
    oapID and puts "  OAP ID: #{oapID}"
    return true
end

###################################################################################################
def genReportHeader()
  str = "OAP ID\t" +
        "Pub ID\t" +
        "Type\t" +
        "Elements URL\t" +
        "Join\t" +
        "Compat\t" +
        "Date\t" +
        "Title\t" +
        "Authors\t"
  ALL_CAMPUSES.each { |campus|
    str += "#{campus} IDs\t"
  }
  $reportFile.puts(str)
end

###################################################################################################
def addToReport(pub, oapID, bestItem)
  isJoinedRecord = db_get_first_value("SELECT isJoinedRecord FROM oap_flags WHERE oap_id = ?", oapID).to_i
  isElemCompat = db_get_first_value("SELECT isElemCompat FROM oap_flags WHERE oap_id = ?", oapID).to_i
  pubID = db_get_first_value("SELECT pub_id FROM pubs WHERE oap_id = ?", oapID)

  str = "#{oapID}\t" +
        "#{pubID}\t" +
        "#{$typeIds[bestItem.typeId]}\t" +
        "#{$elementsUI}/viewobject.html?id=#{pubID}&cid=1\t" +
        "#{isJoinedRecord == 1 ? 'existing' : 'new'}\t" +
        "#{isJoinedRecord == 1 ? isElemCompat : ''}\t" +
        "#{bestItem.date}\t" +
        "#{bestItem.title.inspect}\t" +
        "#{bestItem.authors.join('; ').gsub('|;', ';')}\t"
  campusMap = Hash.new { |h,k| h[k] = Array.new }
  pub.items.map { |item| item.campusIDs }.flatten(1).uniq.each { |scheme, id|
    campusMap[scheme] << id
  }
  ALL_CAMPUSES.each { |campus|
    scheme = "c-#{campus}-id"
    if campusMap.include?(scheme)
      str += campusMap[scheme].join(', ')
    end
    str += "\t"
  }

  $reportFile.puts(str)
end

###################################################################################################
# When a new association between a campus ID and OAP ID occurs, check if it's a possible dupe.
def checkNewAssoc(scheme, campusID, oapID, campusCache)
  if campusCache.empty?
    db_execute("SELECT campus_id FROM ids WHERE oap_id = ?", [oapID]) { |row|
      foundScheme, foundID = row[0].split('::')
      campusCache[foundScheme] << foundID
    }
  end
  if campusCache[scheme].length > 0 && !campusCache[scheme].include?(campusID)
    puts "Warning: possible dupe: campus ID #{scheme}::#{campusID} being added to oapID #{oapID} which already had #{campusCache[scheme].to_a.inspect}."
  end
end

###################################################################################################
# Generate an OAP ID for this publication, then queue it for import.
def mintPub(postNum, pub)

  # Pick the best item for its metadata
  bestItem = pub.items.inject { |memo, item| isBetterItem(item, memo) ? item : memo }

  # Combine all the identifiers and remove duplicates
  ids = pub.items.map { |item| item.ids }.flatten(1).uniq
  campusIDs = pub.items.map { |item| item.campusIDs }.flatten(1).uniq

  # For existing groups, we'll already have an identifier in the database. Re-use it.
  oapID = nil
  campusToOAP = Hash.new{|h, k| h[k] = Hash.new }
  campusIDs.each { |scheme, id|
    old_oapID = db_get_first_value("SELECT oap_id FROM ids WHERE campus_id = ?", "#{scheme}::#{id}")
    campusToOAP[scheme][id] = old_oapID
    oapID ||= old_oapID
  }

  # If we can't find one, mint a new one.
  if not oapID
    idStr = ids.to_a.map { |scheme, id| "#{scheme}::#{id}" }.join(' ')
    puts "[Minting new OAP ID for upcoming post #{postNum}]"
    # Determine the EZID metadata
    meta = { 'erc.what' => normalizeERC("Grouped OA Publication record for '#{bestItem.title}' [IDs #{idStr}]"),
             'erc.who'  => normalizeERC(bestItem.authors.map { |auth| auth.split("|")[0] }.join('; ')),
             'erc.when' => DateTime.now.iso8601 }
    # And mint the ARK
    oapID = mintOAPID(meta)
  end

  # Associate this OAP identifier with all the item IDs so that in future this group will reliably
  # receive the same identifier.
  campusCache = Hash.new { |h,k| h[k] = Set.new }
  campusIDs.each { |scheme, id|
    if campusToOAP[scheme] && campusToOAP[scheme][id]
      if campusToOAP[scheme][id] == oapID
        checkNewAssoc(scheme, id, oapID, campusCache)
        # Unchanged association - don't need to update db
      else
        puts "Warning: campus ID #{scheme}::#{id} is switching from oapID #{campusToOAP[scheme][id]} to #{oapID}"
        checkNewAssoc(scheme, id, oapID, campusCache)
        db_execute("INSERT OR REPLACE INTO ids (campus_id, oap_id) VALUES (?, ?)", ["#{scheme}::#{id}", oapID])
      end
    else
      # New association - add it to the db
      checkNewAssoc(scheme, id, oapID, campusCache)
      #puts "  Inserting new OAP ID."
      db_execute("INSERT INTO ids (campus_id, oap_id) VALUES (?, ?)", ["#{scheme}::#{id}", oapID])
    end
  }

  # Ready for import
  $importQueue << [postNum, pub, ids, bestItem, oapID]
end  

###################################################################################################
# Bring this grouped publication into Elements
def importPub(postNum, pub, ids, bestItem, oapID)
  printed = false

  # Form the XML that we will PUT into Elements
  toPut = makeRecordToPut(bestItem, ids)

  # Check if we've already put the exact same thing.
  newHash = Digest::SHA1.hexdigest toPut
  oldHash = db_get_first_value('SELECT oap_hash FROM oap_hashes WHERE oap_id = ?', oapID)
  isUpdated = false
  if !$forceMode && (oldHash == newHash)
    #puts "  Skip: Existing record is the same."
  else
    printed ||= printPub(postNum, pub, oapID)
    isJoinedRecord, isElemCompat, elemItem = putRecord(pub, oapID, toPut)
    db_execute('INSERT OR REPLACE INTO oap_flags (oap_id, isJoinedRecord, isElemCompat) VALUES (?, ?, ?)',
      [oapID, isJoinedRecord ? 1 : 0, isElemCompat ? 1 : 0])
    isUpdated = true
  end

  # Now associate this record with each author we haven't associated before
  anyNewUsers = false
  oldUsers = db_get_first_value('SELECT oap_users FROM oap_hashes WHERE oap_id = ?', oapID)
  oldUsers = Set.new(oldUsers ? oldUsers.split('|') : []) 
  pub.userPropIds.each { |userPropID|
    if !$forceMode && oldUsers.include?(userPropID)
      #puts "  Skip: User #{userPropID} already associated."
    else
      printed ||= printPub(postNum, pub, oapID)
      postRelationship(oapID, userPropID)
      anyNewUsers = true
    end
  }

  # Keep our database up-to-date in terms of what's been sent already to Elements, so we can avoid
  # re-doing work in the future.
  if isUpdated or anyNewUsers
    db_execute('INSERT OR REPLACE INTO oap_hashes (oap_id, updated, oap_hash, oap_users) VALUES (?, ?, ?, ?)',
      [oapID, DateTime.now.iso8601, newHash, pub.userPropIds.to_a.join('|')])
  end

  # Reporting
  if $reportMode
    addToReport(pub, oapID, bestItem)
  end

  # For testing, stop on first non-joined or non-compat record
  if isUpdated
    if isJoinedRecord && !isElemCompat
      puts "NOTE: incompatible join - see log for details."
      $transLog.puts "NOTE: incompatible join: origItems=#{pub.items.join(' ^^ ')} elemItem=#{elemItem}"
    elsif !isJoinedRecord
      puts "NOTE: record not joined - see log for details."
      $transLog.puts "NOTE: record not joined: #{bestItem}"
    end
  end

  $transLog.flush
  #print "Record done. Hit Enter to do another: "
  #STDIN.gets
end

###################################################################################################
def processMints
  loop do
    postNum, pub = $mintQueue.pop
    break if postNum == "END"
    mintPub(postNum, pub)
  end
  $importQueue << "END"
end

###################################################################################################
def processImports
  postNum = 0
  loop do
    (postNum, pub, ids, bestItem, oapID) = $importQueue.pop
    break if postNum == "END"
    importPub(postNum, pub, ids, bestItem, oapID)
    if (postNum % 1000) == 0
      puts "Checked #{postNum} of #{$toPost.size} posts."
    end
  end
  puts "Checked #{postNum} of #{$toPost.size} posts."
end

###################################################################################################
# Top-level driver
def main

  # We'll need credentials for talking to EZID
  (ezidCred = Netrc.read['ezid.cdlib.org']) or raise("Need credentials for ezid.cdlib.org in ~/.netrc")
  puts "Starting EZID session."
  shoulder = case $hostname
    when 'submit-stg', 'submit-dev'; '99999/fk4'
    when 'cdl-submit-p01'; '13030/p4'
    else 'http://unknown-host/elements-secure-api'
  end
  $ezidSession = Ezid::ApiSession.new(ezidCred[0], ezidCred[1], :ark, shoulder, 'https://ezid.cdlib.org')

  # Need credentials for talking to the Elements API
  ($apiCred = Netrc.read[URI($elementsAPI).host]) or raise("Need credentials for #{URI($elementsAPI).host} in ~/.netrc")

  # Read the primary data, parse it, and build our hashes
  puts "Reading and adding items."
  ARGV.each { |filename|
    addItems(readItems(filename))
  }

  # Print out things we're treating as series titles
  cutoff = 5
  $titleCount.sort.each { |title, count|
    if isSeriesTitle(title)
      if count < cutoff
        #puts "Treating as series but #{count} is below cutoff: #{title}"
      else
        #puts "Series title: #{title}"
      end
    else
      if count >= cutoff
        #puts "Not treating as series title but #{count} is above cutoff: #{title}"
      end
    end
  }

  # Group items by key
  puts "Grouping items."
  groupItems()

  puts "Counting by campus."
  countByCampus = Hash.new{|h, k| h[k] = 0 }
  $pubs.each { |pub|
    schemes = Set.new
    pub.items.each { |item|
      item.ids.each { |scheme, id|
        schemes.include?(scheme) or countByCampus[scheme] = countByCampus[scheme] + 1
        schemes << scheme
      }
    }
  }
  puts "Number to post, by id scheme: #{countByCampus}"

  puts "Counting total to check this run."
  $toPost = []
  $pubs.each { |pub|
    # Match email addresses to Elements users
    pub.items.each { |item|
      item.authors.each { |author|
        email = author.gsub(/.*\|/, '').downcase.strip
        propId = $emailToElementsUser[email]
        if propId
          (pub.userEmails ||= Set.new) << email
          (pub.userPropIds ||= Set.new) << propId
        end
      }
    }

    # If no user ids, there's no point in uploading the item
    pub.userPropIds or next

    # If only doing one campus, skip everything else.
    if $onlyCampus
      next unless pub.items.any?{ |item| item.campusIDs.any?{ |scheme,id| scheme == "c-#{$onlyCampus}-id" }}
    end

    $toPost << pub
  }

  # In report more, generate a report CSV file
  if $reportMode
    puts "Creating report.csv."
    $reportFile = open("report.csv", "w:utf-8")
    genReportHeader
  end

  # Fire up threads for minting and importing
  Thread.abort_on_exception = true
  mintThread = Thread.new {
    processMints
  }
  importThread = Thread.new {
    processImports
  }

  # Check and post each group
  puts "Checking #{$toPost.size} posts."
  postNum = 0
  $toPost.each { |pub|
    postNum += 1
    $mintQueue << [postNum, pub]
  }
  $mintQueue << "END"

  mintThread.join
  importThread.join

  # Finish off report (in report mode)
  $reportMode and $reportFile.close

  puts "All done."
end

# Record the actions in the transaction log file
FileUtils::mkdir_p('log')
open("log/trans-#{DateTime.now.strftime('%F')}.log", "a:utf-8") { |io|
  $transLog = io
  main()
}
