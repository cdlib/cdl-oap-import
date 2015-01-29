###################################################################################################
# Use the right paths to everything, basing them on this script's directory.
def getRealPath(path) Pathname.new(path).realpath.to_s; end
$homeDir    = ENV['HOME'] or raise("No HOME in env")
$scriptDir  = getRealPath "#{__FILE__}/.."
$subiDir    = getRealPath "#{$scriptDir}/.."
$espylib    = getRealPath "#{$subiDir}/lib/espylib"

###################################################################################################
# External code modules
require 'cgi'
require 'date'
require 'fileutils'
require 'nokogiri'
require 'open3'
require 'open-uri'
require 'pp'
require 'netrc'
require 'sqlite3'
require "#{$espylib}/subprocess.rb"
require "#{$espylib}/xmlutil.rb"
require "#{$espylib}/stringutil.rb"

# Flush stdout after each write
STDOUT.sync = true

###################################################################################################
# Determine the Elements API instance to connect to, based on the host name
$hostname = `/bin/hostname`.strip
$elementsAPI = case $hostname
  when 'submit-stg', 'submit-dev'; 'https://qa-oapolicy.universityofcalifornia.edu:8002/elements-secure-api'
  when 'cdl-submit-p01'; 'https://oapolicy.universityofcalifornia.edu:8002/elements-secure-api'
  else raise("unknown host #{$hostname}")
end

# There's another URL for the repository tools API
$repoToolsAPI = case $hostname
  when 'submit-stg', 'submit-dev'; 'http://qa-oapolicy.universityofcalifornia.edu:9090/publications-atom'
  when 'cdl-submit-p01'; 'http://oapolicy.universityofcalifornia.edu:9090/publications-atom'
  else raise("unknown host #{$hostname}")
end

###################################################################################################
def scanAll()

  # Form the URL for getting the first page of pubs in Elements.
  url = "#{$elementsAPI}/publications?detail=full&page=1"

  # Read in credentials we'll need to connect to the API
  credentials = Netrc.read
  elemServer = url[%r{//([^/:]+)}, 1]
  user, passwd = credentials[elemServer]
  passwd or raise("No credentials found in ~/.netrc for machine '#{elemServer}'")

  # Now scan through each page
  while url
    puts "#{DateTime.now.iso8601}: #{url}"
    open(url, :http_basic_authentication=>[user, passwd]) { |io| 
      # We remove namespaces below because it just makes everything easier.
      feedData = Nokogiri::XML(io).remove_namespaces!.root
      feedData.xpath("entry").each { |e| scanPub(e, user, passwd) }
      url = feedData.at("link[rel='next']/@href").try{|hr| hr.to_s}
    }
  end

  puts "Scan complete."
end

###################################################################################################
def scanPub(entry, user, passwd)

  # Grab the Elements publication ID
  pubID = entry.text_at("object/@id")
  pubID or raise "Error: feed entry has no id: #{entry}"

  # Write it to a file named for the pub ID
  dir = "crawled/#{pubID[0,2]}/#{pubID[2,2]}"
  path = "#{dir}/#{pubID}.xml"
  if File.exist?(path)
    puts "Already have #{path}"
    return
  end

  FileUtils::mkdir_p(dir)
  puts "#{path}"
  File.open(path, "w") { |io| entry.write_xml_to io }
end

###################################################################################################
# The main action
scanAll()

