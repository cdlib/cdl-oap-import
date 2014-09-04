#!/usr/bin/env bash

set -e
if [ ! -e oap.db ]; then
  echo "Initializing sqlite database."
  rm -f oap.db.tmp
  sqlite3 oap.db.tmp < db_init.sql
  mv oap.db.tmp oap.db
fi
echo "Installing gems locally."
bundle install --path=gems --binstubs
