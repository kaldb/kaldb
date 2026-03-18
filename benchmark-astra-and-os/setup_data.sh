#!/usr/bin/env bash
set -eu
# Generate files to load into the bulk ingest APIs.
# --------------------------------------------------

# download dataset
mkdir -p data
pushd data || exit
# If the dataset is not already there, download it
[ -f documents.json.bz2 ] || \
curl https://dbyiw3u3rf9yr.cloudfront.net/corpora/nyc_taxis/documents.json.bz2 -O

# generate the split datasets
mkdir -p tmp

split_size=7500
splits_count=10000

_split_lines=$((split_size * splits_count))
bzcat documents.json.bz2 | head -n $_split_lines | split -l $split_size -d -a 4 - tmp/nyc_taxis_
#ctl c after a few seconds

for f in tmp/*; do
  sed -e 's/^/{ "index" : {"_index" : "test"} }\n/' "$f" > "$f".ndjson
done
mkdir -p ready
mv tmp/*.ndjson ready/
rm -r tmp

popd