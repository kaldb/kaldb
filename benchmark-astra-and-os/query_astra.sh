#!/usr/bin/env bash
set -eu

curl -s --fail -X POST -H 'Content-Type: application/json' \
  'http://localhost:8080/_msearch' --data-binary @$1
