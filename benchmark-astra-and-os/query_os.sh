#!/usr/bin/env bash
set -eu

curl -s --fail -ku admin:$OS_PW -XGET "https://localhost:9200/_msearch" \
  -H 'Content-Type: application/json' --data-binary @$1
