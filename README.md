Library of code to de-dupe content from eScholarship and campus sources, assign IDs to the
resulting Open Access Publication records, and push into the Symplectic Elements API.

Test usage:

``./setup.sh
bundle exec ruby groupOapPubs.rb eschol_auth_title.dump | more``

(takes a minute or two to run, prints out diagnostic info and then the dupes found)
