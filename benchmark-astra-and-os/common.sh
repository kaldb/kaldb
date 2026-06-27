
# Shared defaults across the setup and benchmark scripts.

export DATASET="${DATASET:-clickbench_hits}"
export ASTRA_DEPLOYMENT="${ASTRA_DEPLOYMENT:-split}"

export OS_SCHEME="${OS_SCHEME:-http}"
export OS_HOST="${OS_HOST:-localhost}"
export OS_PORT="${OS_PORT:-9200}"
export OS_USER="${OS_USER:-}"
export OS_PW="${OS_PW:-}"
export OS_CURL_INSECURE="${OS_CURL_INSECURE:-false}"
export OS_REQUEST_CACHE="${OS_REQUEST_CACHE:-false}"

export ASTRA_QUERY_HOST="${ASTRA_QUERY_HOST:-localhost}"
export ASTRA_QUERY_PORT="${ASTRA_QUERY_PORT:-8081}"
export ASTRA_BULK_HOST="${ASTRA_BULK_HOST:-localhost}"
export ASTRA_BULK_PORT="${ASTRA_BULK_PORT:-8086}"
export ASTRA_BULK_PATH="${ASTRA_BULK_PATH:-/_bulk}"
export ASTRA_BULK_CURL_MAX_TIME="${ASTRA_BULK_CURL_MAX_TIME:-180}"
export ASTRA_MANAGER_HOST="${ASTRA_MANAGER_HOST:-localhost}"
export ASTRA_MANAGER_PORT="${ASTRA_MANAGER_PORT:-8083}"
export ASTRA_DATASET_OWNER="${ASTRA_DATASET_OWNER:-benchmark}"
export ASTRA_DATASET_THROUGHPUT_BYTES="${ASTRA_DATASET_THROUGHPUT_BYTES:-1000000000}"
export ASTRA_PARTITION_ID="${ASTRA_PARTITION_ID:-0}"
export ASTRA_PARTITION_MAX_CAPACITY_BYTES="${ASTRA_PARTITION_MAX_CAPACITY_BYTES:-10000000000}"
export GRPCURL_IMAGE="${GRPCURL_IMAGE:-fullstorydev/grpcurl:latest}"

case "$DATASET" in
  clickbench_hits)
    export INDEX_NAME="${INDEX_NAME:-hits}"
    export DATA_DIR="${DATA_DIR:-data/clickbench_hits/ready}"
    ;;
  *)
    echo "Unsupported DATASET: $DATASET. This harness currently supports clickbench_hits." >&2
    exit 1
    ;;
esac

os_curl_auth_args=()
if [ -n "$OS_USER" ] || [ -n "$OS_PW" ]; then
  os_curl_auth_args=(-u "${OS_USER:-admin}:$OS_PW")
fi

os_curl_tls_args=()
if [ "$OS_CURL_INSECURE" = "true" ]; then
  os_curl_tls_args=(-k)
fi
