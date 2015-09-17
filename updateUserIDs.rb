#!/usr/bin/env ruby

# This script groups publications together using title, author and (soon) other information like IDs.
# The resulting de-duped "OA Publications" will be used for OAP identifier assignment and then pushing
# into Symplectic Elements via the API.
#
# The code is very much a work in progress.

# System libraries
require 'nokogiri'
require 'sqlite3'

STDOUT.sync = true    # Flush stdout after each write

###################################################################################################
# Global variables

###################################################################################################
# Monkey patches to make Nokogiri even more elegant
class Nokogiri::XML::Node
  def text_at(xpath)
    at(xpath) ? at(xpath).text : nil
  end
end

###################################################################################################
def updateEmails(filename, db)
  db.busy_timeout = 30000
  doc = Nokogiri::XML(filename =~ /\.gz$/ ? Zlib::GzipReader.open(filename) : File.open(filename), &:noblanks)
  doc.remove_namespaces!
  doc.xpath('records/*').each { |record|
    record.name == 'record' or raise("Unknown record type '#{record.name}'")
    email = record.text_at("field[@name='[Email]']") or raise("Record missing email field: #{record}")
    email = email.downcase.strip
    propID = record.text_at("field[@name='[Proprietary_ID]']") or raise("Record missing proprietary ID field: #{record}")

    # Experiment: change foo@dept.ucla.edu to foo@ucla.edu
    # Turns out this is a bad idea. There are different people with the same email in different
    # departments. Example: amahajan@mednet.ucla.edu, amahajan@ucla.edu, amahajan@econ.ucla.edu
    # are all different people.
    #email2 = email.sub(/@.+\.(\w+\.\w+)$/, '@\\1')
    #email2 != email and emails.include?(email2) and puts("Bad: dupe email #{email2}")
    #email2 != email and emails << email2

    existing = db.get_first_value("SELECT proprietary_id FROM emails WHERE email=?", [email])
    if existing == propID
      #puts "No update for #{email} -> #{propID}"
    elsif existing
      puts "Update #{email}: old=#{existing} new=#{propID}"
      db.execute("UPDATE emails SET proprietary_id = ? WHERE email = ?", [propID, email])
    else
      puts "Insert #{email} -> #{propID}"
      db.execute("INSERT INTO emails VALUES (?,?)", [email, propID])
    end
  }
end

###################################################################################################
# Command-line driver
updateEmails(ARGV[0], SQLite3::Database.new("oap.db"))
