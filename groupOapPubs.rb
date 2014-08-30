#!/usr/bin/env ruby

# This script groups publications together using title, author and (soon) other information like IDs.
# The resulting de-duped "OA Publications" will be used for OAP identifier assignment and then pushing
# into Symplectic Elements via the API.
#
# The code is very much a work in progress.

# System libraries
require 'fileutils'
require 'nokogiri'
require 'ostruct'
require 'set'
require 'zlib'

# Flush stdout after each write
STDOUT.sync = true

# Global variables
$allItems = []
$docKeyToItems = Hash.new{|h, k| h[k] = Array.new }
$titleCount = Hash.new{|h, k| h[k] = 0 }
$authKeys = Hash.new{|h, k| h[k] = calcAuthorKeys(k) }
$pubs = []

# Common English stop-words
$stopwords = Set.new(("a an the of and to in that was his he it with is for as had you not be her on at by which have or " +
                      "from this him but all she they were my are me one their so an said them we who would been will no when").split)

# Structure for holding information on an item
Item = Struct.new(:title, :docKey, :authors, :date, :ids, :journal, :volume, :issue)

# Structure for holding a group of duplicate items
OAPub = Struct.new(:items)

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
  tmp = transliterate(str)
  tmp = tmp.gsub(/&lt[;,]/, '<').gsub(/&gt[;,]/, '>').gsub(/&#?\w+[;,]/, '')
  tmp = tmp.gsub(/<[^>]+>/, ' ').gsub(/\\n/, ' ')
  tmp = tmp.gsub(/[|]/, '')
  tmp = tmp.gsub(/\s\s+/,' ').strip
  return tmp
end

###################################################################################################
# Title-specific normalization
def filterTitle(title)
  # Break it into words, and remove the stop words.
  normalize(title).downcase.gsub(/[^a-z0-9 ]/, '').split.select { |w| !$stopwords.include?(w) }
end

###################################################################################################
# Author-specific normalization
def filterAuthors(authors)

  # The individual authors are delimited by semicolons
  authors.split(";").map { |author|

    # Attempt to get the last name first.
    if author =~ /^([^,]+?) (\w+)$/
      puts "Swap author: #{author}"
      author = normalize($2 + " " + $1)
    else
      author = normalize(author)
    end

    # Take the first 4 alpha characters of the result
    author.downcase.gsub(/[^a-z]/,'')[0,4]
  }.sort() # Put authors in sorted order for easier matching
end

###################################################################################################
def calcAuthorKeys(item)
  Set.new(item.authors.map { |auth|
    normalize(auth).downcase.gsub(/[^a-z]/,'')[0,4]
  })
end  

###################################################################################################
# Determine if the title is a likely series item
def isSeriesTitle(title)
  $titleCount[title] == 1 and return false
  ft = title.downcase.gsub(/^[\[\(]|[\)\]]$/, '').gsub(/\s\s+/, ' ').gsub('’', '\'').strip()
  return $seriesTitlesPat.match(ft)
end

###################################################################################################
# See if the candidate is compatible with the existing items in the group
def isCompatible(items, cand)
  candAuthKeys = $authKeys[cand]
  return true if candAuthKeys.empty?
  ok = true
  items.each { |item|
    itemAuthKeys = $authKeys[item]
    next if itemAuthKeys.empty?
    overlap = itemAuthKeys & candAuthKeys
    #overlap.empty? and puts "No overlap: #{itemAuthKeys.to_a.join(',')} vs. #{candAuthKeys.to_a.join(',')}"
    overlap.empty? and ok = false
  }
  return ok
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
    puts "Reading #{filename}."
    doc = Nokogiri::XML(filename =~ /\.gz$/ ? Zlib::GzipReader.open(filename) : File.open(filename), &:noblanks)
    puts "Parsing."
    doc.remove_namespaces!
    nParsed = 0
    doc.xpath('records/*').each { |record|
      record.name == 'import-record' or raise("Unknown record type '#{record.name}'")
      native = record.at('native')

      # Title parsing and doc key generation
      title = normalize(native.text_at("field[@name='title']"))
      docKey = filterTitle(title).join(' ')

      # Author parsing
      authors = native.xpath("field[@name='authors']/people/person").map { |person|
        lname = normalize(person.text_at('last-name'))
        initials = normalize(person.text_at('initials'))
        email = normalize(person.text_at('email'))
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
      ids = native.xpath("field[@name='external-identifiers']/identifiers/identifier").map { |ident|
        [ident['scheme'], ident.text]
      }

      # Journal/vol/iss
      journal = native.text_at("field[@name='journal']")
      volume  = native.text_at("field[@name='volume']")
      issue   = native.text_at("field[@name='issue']")

      # Bundle up the result
      item = Item.new(title, docKey, authors, date, ids, journal, volume, issue)
      items << item
      #puts item

      # Give feedback every once in a while.
      nParsed += 1
      if (nParsed % 1000) == 0
        puts "...#{nParsed} parsed."
      end
    }

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
# For items with a matching title key, group together by overlapping authors to form the OA Pubs.
def groupItems()
  $docKeyToItems.sort.each { |docKey, items|

    # If there are 5 or more dates involved, this is probably a series thing.
    numDates = items.map{ |info| info.date }.uniq.length

    # Singleton cases.
    if items.length == 1 || numDates >= 5 || isSeriesTitle(items[0].title)
      if numDates >= 5
        if !isSeriesTitle(items[0].title)
          puts "Probable series (#{numDates} dates): #{items[0].title}"
        else
          puts "Series would have been covered by date-count #{numDates}: #{items[0].title}"
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

      # See all the kinds of ids we got
      idKinds = Set.new
      pub.items.each { |item|
        item.ids.each { |kind, id|
          idKinds << kind.gsub(/^(uci|ucla|ucsf)_id/, 'campus_id')
        }
      }

      if pub.items.length > 1 && idKinds.include?('eschol_id') && idKinds.include?('campus_id')
        puts
        pub.items.each { |item|
          idStr = item.ids.map { |kind, id| "#{kind}::#{id}" }.join(', ')
          puts "#{item.title}"
          puts "    #{item.date}     #{item.authors.join(', ')}"
          puts "    #{idStr}"
        }
      end

      # Done with this grouped publication
      $pubs << pub
    end
  }
end

###################################################################################################
# Top-level driver
def main

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
        puts "Treating as series but #{count} is below cutoff: #{title}"
      else
        puts "Series title: #{title}"
      end
    else
      if count >= cutoff
        puts "Not treating as series title but #{count} is above cutoff: #{title}"
      end
    end
  }

  # Group items by key
  puts "Grouping items."
  groupItems()

  # Print the interesting (i.e. non-singleton) groups
  #$pubs.each { |pub|
  #  if pub.items.length > 1
  #    puts
  #    pub.items.each { |item|
  #      puts "#{item.ids}  #{item.title}"
  #      puts "    #{item.date}     #{item.authors}"
  #    }
  #  end
  #}
end

main()