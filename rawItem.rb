
# Structure for holding information on an item
RawItem = Struct.new(:typeName, :title, :docKey, :updated, :authors, :suggestions, :date, :ids, :journal, :volume, :issue, :otherInfo) {
  def self.load(campusID)
    itemData = db_get_first_value("SELECT item_data FROM raw_items WHERE campus_id = ?", campusID.join("::"))
    itemData or raise("Can't find database record for campus ID #{campusID.join('::')}")
    return Marshal.load(itemData)
  end

  def save(db_execute)
    row_id = ids.select { |scheme, text| scheme == 'elements' }[0]
    if not row_id
      campusIDs.length == 1 or raise("Ambiguous campus ID, can't save to database.")
      row_id = campusIDs[0]
    end
    db_execute.call("INSERT OR REPLACE INTO raw_items (campus_id, doc_key, updated, item_data) VALUES (?, ?, ?, ?)",
                    [row_id.join("::"), docKey, updated, Marshal.dump(self)])
  end

  def campusIDs
    ids.select { |scheme, text| isCampusID(scheme) }
  end

  def isFromElements
    ids.any? { |scheme, text| scheme=="elements" }
  end
}

# Less common item info (only for books, conferences, etc.)
OtherItemInfo = Struct.new(:abstract, :editors, :publisher, :placeOfPublication, :pagination, :nameOfConference, :parentTitle)

# Common English stop-words
$stopwords = Set.new(("a an the of and to in that was his he it with is for as had you not be her on at by which have or " +
                      "from this him but all she they were my are me one their so an said them we who would been will no when").split)

# Transliteration tables -- a cheesy but easy way to remove accents without requiring a Unicode gem
$transFrom = "ÀÁÂÃÄÅàáâãäåĀāĂăĄąÇçĆćĈĉĊċČčÐðĎďĐđÈÉÊËèéêëĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħÌÍÎÏìíîïĨĩĪīĬĭĮįİıĴĵĶķĸĹĺĻļĽ" +
             "ľĿŀŁłÑñŃńŅņŇňŉŊŋÒÓÔÕÖØòóôõöøŌōŎŏŐőŔŕŖŗŘřŚśŜŝŞşŠšſŢţŤťŦŧÙÚÛÜùúûüŨũŪūŬŭŮůŰűŲųŴŵÝýÿŶŷŸŹźŻżŽž"
$transTo   = "AAAAAAaaaaaaAaAaAaCcCcCcCcCcDdDdDdEEEEeeeeEeEeEeEeEeGgGgGgGgHhHhIIIIiiiiIiIiIiIiIiJjKkkLlLlL" +
             "lLlLlNnNnNnNnnNnOOOOOOooooooOoOoOoRrRrRrSsSsSsSssTtTtTtUUUUuuuuUuUuUuUuUuUuWwYyyYyYZzZzZz"

ALL_CAMPUSES = ['eschol', 'ucla', 'uci', 'ucsf']

def isCampusID(scheme)
  return scheme =~ /^c-/
end

# Elements type IDs
$typeIdToName = { 2 => 'book',
                  3 => 'chapter',
                  4 => 'conference',
                  5 => 'journal article',
                  6 => 'patent',
                  7 => 'report',
                  8 => 'software / code',
                  9 => 'performance',
                  10 => 'composition',
                  11 => 'design',
                  12 => 'artefact',
                  13 => 'exhibition',
                  14 => 'other',
                  15 => 'internet publication',
                  16 => 'scholarly edition',
                  17 => 'poster',
                  18 => 'thesis / dissertation',
                  22 => 'dataset',
                  50 => 'figure',
                  51 => 'fileset',
                  52 => 'media',
                  53 => 'presentation'
                }
$typeNameToID = $typeIdToName.invert

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
# Title-specific normalization for generating a document key
def filterTitle(title)
  # Break it into words, and remove the stop words.
  key = transliterate(normalize(title)).downcase.gsub(/[^a-z0-9 ]/, ' ').split.select { |w| !$stopwords.include?(w) }

  # If we ended up keeping at least half the title, that's the key
  if key.join(" ").length >= (title.length/2)
    return key
  end

  # Otherwise, use a simpler, safer method. This is needed e.g. for titles that are all non-latin characters, or
  # all stop words like "To Be Or Not To Be" (a real title!)
  return normalize(title).downcase.split
end

###################################################################################################
def calcAuthorKeys(item)
  Set.new(item.authors.map { |auth|
    transliterate(normalize(auth)).downcase.gsub(/[^a-z]/,'')[0,4]
  })
end

###################################################################################################
def elemNativeToRawItem(native, typeName, updated)

  # Type validation (book, article, etc.)
  $typeNameToID.include?(typeName) or raise("Unknown typeName #{typeName.inspect}")

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
  return RawItem.new(typeName, title, docKey, updated, authors, nil, date, ids, journal, volume, issue, (other.select{|v| v}.empty?) ? nil : other)
end

