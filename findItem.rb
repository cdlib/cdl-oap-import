#!/usr/bin/env ruby

# This script attempts to match incoming IDs to things in the OAP database, and
# prints out whatever it can.

###################################################################################################
# Use the right paths to everything, basing them on this script's directory.
def getRealPath(path) Pathname.new(path).realpath.to_s; end
$homeDir    = ENV['HOME'] or raise("No HOME in env")
$scriptDir  = getRealPath "#{__FILE__}/.."
$subiDir    = getRealPath "#{$scriptDir}/.."
$espylib    = getRealPath "#{$subiDir}/lib/espylib"
$erepDir    = getRealPath "#{$subiDir}/xtf-erep"
$arkDataDir = getRealPath "#{$erepDir}/data"
$controlDir = getRealPath "#{$erepDir}/control"

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

require_relative 'rawItem'

# Flush stdout after each write
STDOUT.sync = true

# Global variables
$oapDb = SQLite3::Database.new("#{$scriptDir}/oap.db")
$oapDb.busy_timeout = 30000

$arkDb = SQLite3::Database.new("#{$controlDir}/db/arks.db")
$arkDb.busy_timeout = 30000

###################################################################################################
# Determine the Elements API instance to connect to, based on the host name
$hostname = `/bin/hostname`.strip
$elementsUI = case $hostname
  when 'pub-submit-stg-2a', 'pub-submit-stg-2c'; 'https://qa-oapolicy.universityofcalifornia.edu'
  when 'pub-submit-prd-2a', 'pub-submit-prd-2c'; 'https://oapolicy.universityofcalifornia.edu'
  else 'http://unknown-host'
end
$elementsAPI = "#{$elementsUI}:8002/elements-secure-api"

###################################################################################################
def isEscholArk(ark)
  return false unless ark =~ %r!^ark:/13030/qt\w{8}!
  tmp = ark.sub("ark:/13030/", "ark:13030/")  # kludge for old mistake
  return $arkDb.get_first_value("SELECT COUNT(*) FROM arks WHERE id = ?", tmp) == 1
end

###################################################################################################
# Try different ways to make the ID into a valid eScholarship ARK
def toEscholArk(id)
  return id if isEscholArk(id)
  tmp = id.sub("ark:13030/", "ark:/13030/")
  return tmp if isEscholArk(tmp)
  tmp = "ark:/13030/qt#{id}"
  return tmp if isEscholArk(tmp)
  tmp = "ark:/13030/#{id}"
  return tmp if isEscholArk(tmp)
  return nil
end

###################################################################################################
def printEschol(id)
  ark = toEscholArk(id)
  return unless ark
  puts "  eSchol ID: #{ark}"
  printOap($oapDb.get_first_value("SELECT oap_id FROM ids WHERE campus_id = ?", "c-eschol-id::#{ark}"))
  equivPub = $oapDb.get_first_value("SELECT pub_id FROM eschol_equiv WHERE eschol_ark = ?", ark)
  if equivPub
    puts "  Marked as 'equivalent' to pub #{equivPub}"
    printPub(equivPub)
  end
end

###################################################################################################
def isOapID(id)
  return false unless id =~ %r!^ark:/13030/p4!
  return $oapDb.get_first_value("SELECT COUNT(*) FROM pubs WHERE oap_id = ?", id) == 1
end

###################################################################################################
# Try different ways to make the ID into a valid OAP ARK
def toOapID(id)
  return id if isOapID(id)
  tmp = "ark:/13030/#{id}"
  return tmp if isOapID(tmp)
  return nil
end

###################################################################################################
def printOap(id)
  oapID = toOapID(id)
  return unless oapID
  pubID = toPubID($oapDb.get_first_value("SELECT pub_id FROM pubs WHERE oap_id = ?", oapID))
  if pubID
    printPub(pubID)
  else
    puts "  OAP ID with no linking pub: #{oapID}"
  end
end

###################################################################################################
def isCampusID(id)
  id.length >= 5 or return false
  return $oapDb.get_first_value("SELECT COUNT(*) FROM ids WHERE campus_id = ?", id) == 1 \
      || $oapDb.get_first_value("SELECT COUNT(*) FROM ids WHERE campus_id LIKE ?", "%::#{id}") == 1
end

###################################################################################################
# Try different ways to make the ID into a valid Elements publication ID
def toCampusID(id)
  campusID = $oapDb.get_first_value("SELECT campus_id FROM ids WHERE campus_id = ?", id)
  return campusID if campusID
  campusID = $oapDb.get_first_value("SELECT campus_id FROM ids WHERE campus_id LIKE ?", "%::#{id}")
  return campusID if campusID
  return nil
end

###################################################################################################
def printCampusID(id)
  campusID = toCampusID(id)
  return unless campusID
  puts "  Campus ID #{campusID}"
  printOap($oapDb.get_first_value("SELECT oap_id FROM ids WHERE campus_id = ?", campusID))
end

###################################################################################################
def isPubID(id)
  return false unless id =~ %r!^\d+$!
  return $oapDb.get_first_value("SELECT COUNT(*) FROM pubs WHERE pub_id = ?", id) == 1
end

###################################################################################################
# Try different ways to make the ID into a valid Elements publication ID
def toPubID(id)
  return id if isPubID(id)
  return nil
end

###################################################################################################
def db_get_first_value(stmt, *more)
  return $oapDb.get_first_value(stmt, *more)
end

###################################################################################################
def printPub(id)
  pubID = toPubID(id)
  oapID = $oapDb.get_first_value("SELECT oap_id FROM pubs WHERE pub_id = ?", pubID)
  return unless pubID
  
  puts "  Pub #{pubID} <=> OAP #{oapID}"

  # Print flags
  anyFlags = false
  $oapDb.execute("SELECT isJoinedRecord, isElemCompat FROM oap_flags WHERE oap_id = ?", oapID).each { |flags|
    if flags[0] == 1
      puts "    Joined to harvested record in Elements. Compatibly=#{flags[1]==1 ? 'yes' : 'no'}"
    else
      puts "    Not joined"
    end
    anyFlags = true
  }
  if !anyFlags
    puts "    NOTE: nothing in oap_flags for this pub."
  end

  # Print import hash info
  anyImports = false
  $oapDb.execute("SELECT updated, oap_users FROM oap_hashes WHERE oap_id = ?", oapID).each { |imports|
    #puts "    Updated to Elements: #{imports[0]}"
    #puts "    Users in Elements  : #{imports[1].split("|").join(", ")}"
    anyImports = true
  }
  if !anyImports
    puts "    NOTE: nothing in oap_hashes for this pub."
  end

  # Print out 'equivalent' eschol ARKs
  $oapDb.execute("SELECT eschol_ark FROM eschol_equiv WHERE pub_id = ?", pubID).each { |row|
    equivArk = row[0]
    puts "    Marked as 'equivalent' to eschol #{equivArk}"
  }

  # Print out campus IDs
  anyCampusIDs = false
  $oapDb.execute("SELECT campus_id FROM ids WHERE oap_id = ? ORDER BY campus_id", oapID).each { |row|
    campusID = row[0]
    puts "    #{campusID}"
    if $oapDb.get_first_value("SELECT COUNT(*) FROM raw_items WHERE campus_id = ?", campusID) == 1
      item = RawItem.load(campusID.split("::"))
      puts "      type : #{item.typeName.inspect}"
      puts "      title: #{item.title.inspect}"
    else
      puts "      NOTE: can't find raw_item."
    end
    anyCampusIDs = true
  }
  if !anyCampusIDs
    puts "    NOTE: no campus IDs associated with this pub."
  end
end

###################################################################################################
def main
  ARGV.each { |arg|
    next if arg.length <= 5
    puts "-----------------------------------------------------------------------------------------------"
    puts "Identifying #{arg.inspect}."
    tmp = toEscholArk(arg)
    if tmp
      printEschol(tmp)
      next
    end
    tmp = toOapID(arg)
    if tmp
      printOap(tmp)
      next
    end
    tmp = toPubID(arg)
    if tmp
      printPub(tmp)
      next
    end
    tmp = toCampusID(arg)
    if tmp
      printCampusID(tmp)
      next
    end
    puts "Error: Can't identify #{arg}"
  }
end

main()
puts "-----------------------------------------------------------------------------------------------"
