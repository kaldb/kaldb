# Scratch file for setup snippets
# Datasets bits
# -----
# download dataset
mkdir -p data
pushd data || exit
# If the dataset is not already there, download it
[ -f documents.json.bz2 ] || \
curl https://dbyiw3u3rf9yr.cloudfront.net/corpora/nyc_taxis/documents.json.bz2 -O


# generating the split datasets
mkdir -p tmp
bzcat documents.json.bz2 | split -l 7500 -d -a 4 - tmp/btmp_
#ctl c after a few seconds
for f in tmp/*; do
  sed -e 's/^/{ "index" : {"_index" : "test"} }\n/' "$f" > "$f".ndjson
done
mkdir -p ready
mv tmp/*.ndjson ready/
rm -r tmp

# load the first n files into opensearch and astra
for f in $(ls data/ready/* | head -n 25); do
  echo $f
  curl -H "Content-Type: application/x-ndjson" -XPOST "https://localhost:9200/_bulk" \
     --data-binary @$f  -ku admin:$OS_PW -s > /dev/null
  curl -H "Content-Type: application/x-ndjson" -k -XPOST "http://localhost:8080/_local_bulk" --data-binary @$f -s > /dev/null
done


  curl -H "Content-Type: application/x-ndjson" -XPOST "https://localhost:9200/_bulk" \
     --data-binary @data/ready/btmp_0001.ndjson  -ku admin:$OS_PW -s > /dev/null

# Create temp datasets for testing that hit each index
bzcat documents.json.bz2 | head -n 7500 > test_7500.json
sed -e 's/^/{ "index" : {"_index" : "test"} }\n/' test_7500.json > test_7500.ndjson


bzcat documents.json.bz2 | head -n 187500 > test_187500.json
sed -e 's/^/{ "index" : {"_index" : "test"} }\n/' test_187500.json > test_187500.ndjson



bzcat documents.json.bz2 | head -n 187500 | tail -n 180000 > test_18-7500.json
sed -e 's/^/{ "index" : {"_index" : "test"} }\n/' test_18-7500.json > test_18-7500.ndjson

curl -H "Content-Type: application/x-ndjson" -XPOST "https://localhost:9200/_bulk" \
   --data-binary @data/test_18-7500.ndjson  -ku admin:$OS_PW

curl -H "Content-Type: application/x-ndjson" -k -XPOST "http://localhost:8080/_local_bulk" --data-binary @data/test_18-7500.ndjson


# to check the stats
curl -X GET "https://localhost:9200/test_fixed/_settings?pretty" -ku admin:$OS_PW

# cluster setup bits
# -----

# start opensearch
export OS_PW="W*1hatever" # pw must be “strong enough” which means capital letter, number and symbol and presumably no obvious words
docker run -d -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" \
  -e "OPENSEARCH_INITIAL_ADMIN_PASSWORD=$OS_PW" opensearchproject/opensearch:latest

# create index and load data
curl -X PUT "https://localhost:9200/test" -H "Content-Type: application/json" -ku admin:$OS_PW
curl -H "Content-Type: application/x-ndjson" -XPOST "https://localhost:9200/_bulk" \
  --data-binary @data/tmp_000.indexified \
  -ku admin:$OS_PW

# create index with field mappings
curl -X PUT "https://localhost:9200/test" -H "Content-Type: application/json" -ku admin:$OS_PW \
 -d '{ "mappings": { "properties": { "dropoff_datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss" }}}}'


# delete index
curl -XDELETE "https://localhost:9200/test" -ku admin:$OS_PW

# setup astra
cd ../astra
git checkout zparekh/local_bulk_ingest_api
docker build -t slackhq/astra .
docker compose up


# could create a schema for the astra cluster for better 1:1 perf comparison


for f in $(ls data/ready/* | head -n 75); do
  echo $f
  curl -H "Content-Type: application/x-ndjson" -XPOST "https://localhost:9200/_bulk" \
     --data-binary @$f  -ku admin:$OS_PW -s > /dev/null
  curl -H "Content-Type: application/x-ndjson" -k -XPOST "http://localhost:8080/_local_bulk" --data-binary @$f -s > /dev/null
done
