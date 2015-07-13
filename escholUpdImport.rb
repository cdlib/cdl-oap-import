#!/usr/bin/env ruby

# Update the eScholarship publication data from the index, convert it, and do a new
# import any changed records into Elements.

require_relative 'subprocess'

# Check the date stamp on our index dump. If it's more than 12 hours old, regenerate.
dumpFile = "/apps/eschol/subi/pub-oapi/pub_import/eschol/eschol_pubs.xml.gz"
if !File::exist?(dumpFile) || (Time.now - File.mtime(dumpFile)) > (60*60*12)
  puts "eSchol index dump is more than 12 hours old; updating."
  checkCall("/apps/eschol/erep/xtf/bin/indexDump -index erep -xml -allFields | gzip -c > #{dumpFile}.new")
  File.rename("#{dumpFile}.new", dumpFile)
else
  puts "Index dump is up to date."
end

# If the API feed is out-of-date relative to the index dump, rebuild it.
apiFile = "/apps/eschol/subi/pub-oapi/pub_import/eschol/eschol_pubs_api_feed.xml.gz"
if !File::exist?(apiFile) || File.mtime(apiFile) < File.mtime(dumpFile)
  puts "Transforming index dump to API feed."
  checkCall("gunzip -c #{dumpFile} | sed 's/><\$ / /g' | " +
            "java -Xmx1500m -classpath /apps/eschol/subi/pub-oapi/pub_import/lib/saxonb-8.9.jar net.sf.saxon.Transform - /apps/eschol/subi/pub-oapi/pub_import/eschol/eschol2api.xsl | " +
            "gzip -c > #{apiFile}.new")
  File.rename("#{apiFile}.new", apiFile)
else
  puts "API feed is up to date."
end

# Now perform the import
exec("/apps/eschol/subi/oapImport/run.sh --only eschol")