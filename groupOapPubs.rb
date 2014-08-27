#!/usr/bin/env ruby

# This script groups publications together using title, author and (soon) other information like IDs.
# The resulting de-duped "OA Publications" will be used for OAP identifier assignment and then pushing
# into Symplectic Elements via the API.
#
# The code is very much a work in progress.

# System libraries
require 'ostruct'
require 'set'

# Flush stdout after each write
STDOUT.sync = true

# Global variables
$idToItem = {}
$docKeyToIds = Hash.new{|h, k| h[k] = Set.new }
$titleCount = Hash.new{|h, k| h[k] = 0 }
$pubs = []

# Common English stop-words
$stopwords = Set.new(("a an the of and to in that was his he it with is for as had you not be her on at by which have or " +
                      "from this him but all she they were my are me one their so an said them we who would been will no when").split)

# Structure for holding information on an item
Item = Struct.new(:id, :title, :authors, :entity, :date, :docKey, :ftitle, :fauthors)

# Structure for holding a group of duplicate items
OAPub = Struct.new(:itemIds)

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
# Remove accents from a string
def transliterate(str)
  str.tr($transFrom, $transTo)
end

###################################################################################################
# Convert to lower case, remove HTML-like elements, strip out weird characters, normalize spaces.
def normalize(str)
  tmp = transliterate(str)
  tmp = tmp.downcase.gsub(/&lt[;,]/, '<').gsub(/&gt[;,]/, '>').gsub(/&#?\w+[;,]/, '')
  tmp = tmp.gsub(/<[^>]+>/, ' ').gsub(/\\n/, ' ').gsub(/[^a-z0-9 ]/, '')
  tmp = tmp.gsub(/\s\s+/,' ').strip
  return tmp
end

###################################################################################################
# Title-specific normalization
def filterTitle(title)
  # Break it into words, and remove the stop words.
  normalize(title).split.select { |w| !$stopwords.include?(w) }
end

###################################################################################################
# Author-specific normalization
def filterAuthors(authors)

  # The individual authors are delimited by semicolons
  authors.split(";").map { |author|

    # Attempt to get the last name first.
    if author =~ /^([^,]+?) (\w+)$/
      author = normalize($2 + " " + $1)
    else
      author = normalize(author)
    end

    # Take the first 4 alpha characters of the result
    author.gsub(/[^a-z]/,'')[0,4]
  }.sort() # Put authors in sorted order for easier matching
end

###################################################################################################
# Determine if the title is a likely series item
def isSeriesTitle(title)
  $titleCount[title] == 1 and return false
  ft = title.downcase.gsub(/^[\[\(]|[\)\]]$/, '').gsub(/\s\s+/, ' ').gsub('’', '\'').strip()
  return $seriesTitlesPat.match(ft)
end

###################################################################################################
# For items with a matching title key, group together by overlapping authors to form the OA Pubs.
def groupItems()
  $docKeyToIds.sort.each { |docKey, ids|
    items = ids.map { |id| $idToItem[id] }

    # If there are 5 or more dates involved, this is probably a series thing.
    numDates = items.map{ |info| info.date }.uniq.length

    # Singleton cases.
    if ids.length == 1 || numDates >= 5 || isSeriesTitle(items[0].title)
      if numDates >= 5
        if !isSeriesTitle(items[0].title)
          puts "Probable series (#{numDates} dates): #{items[0].title}"
        else
          puts "Series would have been covered by date-count #{numDates}: #{items[0].title}"
        end
      end
      
      # Singletons: make a separate OAPub for each item
      ids.each { |id| $pubs << OAPub.new([id]) }
      next
    end

    # We know the docs all share the same title key. Group those that share an author.
    while !items.empty?

      # Get the first item
      item1 = items.shift
      pub = OAPub.new([item1.id])

      # Match it up to every other item that shares at least one author key
      items.dup.each { |item2|
        if item1.authors == item2.authors || (item1.fauthors & item2.fauthors).length > 0
          items.delete(item2)
          pub.itemIds << item2.id
        end
      }

      # Done with this grouped publication
      $pubs << pub
    end
  }
end

###################################################################################################
# Top-level driver
def main

  # Read the primary data, parse it, and build our hashes
  puts "Reading items."
  File.open(ARGV[0], "r:UTF-8").each { |line|
    id, title, authors, contentExists, withdrawn, entity, date = line.split("|")
    next if withdrawn != ""
    next if contentExists != "yes"

    ftitle = filterTitle(title)
    fauthors = filterAuthors(authors)
    docKey = ftitle.join(' ')
    item = Item.new(id, title, authors, entity, date, docKey, ftitle, fauthors)
    $idToItem[id] = item
    $titleCount[title] += 1
    $docKeyToIds[docKey] << id
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
  $pubs.each { |pub|
    if pub.itemIds.length > 1
      puts
      pub.itemIds.each_with_index { |id, index|
        item = $idToItem[id]
        puts "#{id}  #{item.title}"
        puts "    #{item.date}     #{item.entity.ljust(20)}     #{item.authors}"
      }
    end
  }
end

main()