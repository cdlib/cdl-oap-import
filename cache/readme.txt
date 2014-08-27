Data files are read in from XML format, and then saved to this directory in compressed binary form
(gzipped Ruby 'Marshal' format to be exact). If you change the reading logic, blow away the cache
files to force the original XML to be re-read.
