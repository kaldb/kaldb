#!/usr/bin/env bash
set -eu
source common.sh

curl -s --fail -X POST -H 'Content-Type: application/json' \
  "http://${ASTRA_QUERY_HOST}:${ASTRA_QUERY_PORT}/_msearch" --data-binary "@$1"
