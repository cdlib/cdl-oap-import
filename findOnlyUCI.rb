#!/usr/bin/env ruby

#!/usr/bin/env ruby

# This script finds OAP records that are only associated with UCI.

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
require 'zlib'

# Flush stdout after each write
STDOUT.sync = true

$db = SQLite3::Database.new("oap.db")
$db.busy_timeout = 30000

###################################################################################################
# Determine the Elements API instance to connect to, based on the host name
$hostname = `/bin/hostname`.strip
$elementsAPI = case $hostname
  when 'submit-stg', 'submit-dev'; 'https://qa-oapolicy.universityofcalifornia.edu:8002/elements-secure-api'
  when 'cdl-submit-p01'; 'https://oapolicy.universityofcalifornia.edu:8002/elements-secure-api'
  else 'http://unknown-host/elements-secure-api'
end

# Need credentials for talking to the Elements API
($apiCred = Netrc.read[URI($elementsAPI).host]) or raise("Need credentials for #{URI($elementsAPI).host} in ~/.netrc")

def removeOAP(oapID)
  puts "Removing #{oapID}."
  uri = URI("#{$elementsAPI}/publication/records/c-inst-1/#{CGI.escape(oapID)}")

  # And put it
  req = Net::HTTP::Delete.new(uri)
  req.basic_auth $apiCred[0], $apiCred[1]
  res = Net::HTTP.start(uri.hostname, uri.port, :use_ssl => uri.scheme == 'https') { |http|
    http.request req
  }

end

###################################################################################################
# Go for it
idSchemes = Hash.new{|h, k| h[k] = Set.new }
$db.execute("SELECT campus_id, oap_id FROM ids") { |row|
  campusID, oapID = row
  scheme, text = campusID.split("::")
  idSchemes[oapID] << scheme
}

idSchemes.each { |oapID, schemes|
  if schemes.include?('c-uci-id') && schemes.length == 1
    removeOAP(oapID)    
  end
}