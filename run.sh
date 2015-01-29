#!/usr/bin/env bash

bundle exec ruby updateUserIDs.rb hr_feed.xml && bundle exec ruby groupOapPubs.rb uc*.xml* eschol.xml.gz $*
