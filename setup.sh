#!/usr/bin/env bash

bundle install --path=gems --binstubs
if [ ! -e oap_ids.db ]; then
  sqlite3 oap_ids.db < db_init.sql
fi