#!/usr/bin/env bash

set -e
echo "Updating user IDs."
bundle exec ruby updateUserIDs.rb hr_feed.xml
echo "Importing publications."
bundle exec ruby groupOapPubs.rb ucsf*.xml* ucla*.xml* uci*.xml* eschol.xml* $*
