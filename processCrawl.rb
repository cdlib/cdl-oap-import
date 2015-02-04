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

doiToPubs = Hash.new { |h,k| h[k] = Set.new }
pmidToPubs = Hash.new { |h,k| h[k] = Set.new }

# Process each file
Dir.glob("crawled/**/*.xml").each { |fn|
  feed = fileToXML(fn)
  pubID = feed.text_at("object/@id") or raise("no pubID found")
  stuff = feed.xpath(".//field[@name='doi']")
  dois = Set.new(feed.xpath(".//field[@name='doi']/text").map { |el| el.text })
  pmids = Set.new(feed.xpath(".//identifier[@scheme='pubmed']").map { |el| el.text })
  title = feed.text_at("title[1]")
  dois.each { |doi| doiToPubs[doi] << pubID }
  pmids.each { |pmid| pmidToPubs[pmid] << pubID }
}

doiToPubs.each { |doi, pubs|
  next if pubs.length < 2
  puts "doi=#{doi} pubs=#{pubs.inspect}"
}

pmidToPubs.each { |pmid, pubs|
  next if pubs.length < 2
  puts "pmid=#{pmid} pubs=#{pubs.inspect}"
}