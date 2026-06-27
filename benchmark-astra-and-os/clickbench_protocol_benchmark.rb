#!/usr/bin/env ruby

require "benchmark"
require "csv"
require "fileutils"
require "json"
require "net/http"
require "open3"
require "openssl"
require "optparse"
require "time"
require "uri"

require_relative "clickbench_queries"
require_relative "jvm_capture"

SCRIPT_DIR = File.expand_path(__dir__)
REPO_ROOT = File.expand_path("..", SCRIPT_DIR)

PHASES = %w[cold hot1 hot2].freeze

def boolean_env(name, default)
  case ENV.fetch(name, default).downcase
  when "true" then true
  when "false" then false
  else
    abort("#{name} must be true or false")
  end
end

def run_cmd(*cmd, chdir: REPO_ROOT)
  stdout, stderr, status = Open3.capture3(*cmd, chdir: chdir)
  {
    cmd: cmd,
    stdout: stdout,
    stderr: stderr,
    success: status.success?,
    exit_status: status.exitstatus
  }
end

def shell!(command)
  puts "+ #{command}"
  system(command) || abort("Command failed: #{command}")
end

def shell(command)
  puts "+ #{command}"
  system(command)
end

def wait_for_url(name, url, timeout_seconds: 240)
  deadline = Time.now + timeout_seconds
  print "Waiting for #{name}"
  loop do
    uri = URI(url)
    begin
      response = Net::HTTP.start(uri.host, uri.port, open_timeout: 2, read_timeout: 2) do |http|
        http.get(uri.request_uri)
      end
      if response.code.to_i < 500
        puts " ready."
        return
      end
    rescue StandardError
      # Retry below.
    end

    abort("Timed out waiting for #{name}") if Time.now >= deadline

    print "."
    sleep 2
  end
end

def post_json(url, body, user: "", password: "", insecure: false, read_timeout: 1_800)
  uri = URI(url)
  request = Net::HTTP::Post.new(uri)
  request["Content-Type"] = "application/json"
  request.body = body
  request.basic_auth(user.empty? ? "admin" : user, password) if !user.empty? || !password.empty?

  http = Net::HTTP.new(uri.host, uri.port)
  http.open_timeout = 10
  http.read_timeout = read_timeout
  if uri.scheme == "https"
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE if insecure
  end

  response = http.request(request)
  [response.code.to_i, response.body]
end

def response(json)
  json&.dig("responses", 0)
end

def request_has_aggregations?(request)
  request.key?("aggs") || request.key?("aggregations")
end

def request_tracks_total_hits?(request)
  value = request.fetch("track_total_hits", true)
  value != false && value != -1
end

def benchmark_request(request, track_total_hits)
  copied_request = Marshal.load(Marshal.dump(request))
  copied_request["track_total_hits"] = track_total_hits
  copied_request
end

def response_data_notes(subject, resp, request)
  notes = []
  unless resp
    notes << "#{subject}: missing msearch response"
    return notes
  end

  hits = resp["hits"]
  total = hits&.dig("total", "value")
  returned_hits = hits&.fetch("hits", nil)
  requested_size = Integer(request.fetch("size", 10))

  if request_tracks_total_hits?(request)
    if total.nil?
      notes << "#{subject}: missing hits.total.value"
    elsif total.to_i <= 0
      notes << "#{subject}: hits.total.value is #{total}"
    end
  end

  if request_has_aggregations?(request)
    aggregations = resp["aggregations"]
    notes << "#{subject}: missing aggregations" unless aggregations.is_a?(Hash) && !aggregations.empty?
  elsif requested_size.positive?
    notes << "#{subject}: no returned hits" unless returned_hits.is_a?(Array) && !returned_hits.empty?
  end

  notes
end

def numeric_equal?(left, right)
  return true if left == right
  return false unless left.is_a?(Numeric) && right.is_a?(Numeric)

  (left.to_f - right.to_f).abs <= [0.001, right.to_f.abs * 0.000001].max
end

def compare_scalar(left, right, path, diffs)
  if left.is_a?(Numeric) || right.is_a?(Numeric)
    diffs << "#{path}: astra=#{left.inspect} os=#{right.inspect}" unless numeric_equal?(left, right)
    return
  end

  diffs << "#{path}: astra=#{left.inspect} os=#{right.inspect}" unless left == right
end

def iso8601_millis(value)
  return nil unless value.is_a?(String)

  (Time.iso8601(value).to_r * 1000).round
rescue ArgumentError
  nil
end

def compare_sort_scalar(left, right, path, diffs)
  left_millis = iso8601_millis(left)
  right_millis = iso8601_millis(right)
  if left_millis && right.is_a?(Numeric)
    compare_scalar(left_millis, right, path, diffs)
  elsif right_millis && left.is_a?(Numeric)
    compare_scalar(left, right_millis, path, diffs)
  else
    compare_scalar(left, right, path, diffs)
  end
end

IGNORED_AGG_KEYS = [
  "doc_count_error_upper_bound",
  "sum_other_doc_count",
  "key_as_string",
  "value_as_string"
].freeze

def compare_agg_node(astra_node, os_node, path, diffs)
  if astra_node.is_a?(Hash) && os_node.is_a?(Hash)
    if astra_node.key?("value") || os_node.key?("value")
      compare_scalar(astra_node["value"], os_node["value"], "#{path}.value", diffs)
    end

    if astra_node.key?("buckets") || os_node.key?("buckets")
      astra_buckets = astra_node["buckets"]
      os_buckets = os_node["buckets"]
      unless astra_buckets.is_a?(Array) && os_buckets.is_a?(Array)
        diffs << "#{path}.buckets: astra=#{astra_buckets.class} os=#{os_buckets.class}"
        return
      end

      if astra_buckets.size != os_buckets.size
        diffs << "#{path}.buckets size: astra=#{astra_buckets.size} os=#{os_buckets.size}"
        return
      end

      astra_buckets.zip(os_buckets).each_with_index do |(astra_bucket, os_bucket), index|
        compare_agg_node(astra_bucket, os_bucket, "#{path}.buckets[#{index}]", diffs)
        return if diffs.any?
      end
    end

    keys = (astra_node.keys | os_node.keys) - IGNORED_AGG_KEYS - ["buckets", "value"]
    keys.sort.each do |key|
      if !astra_node.key?(key)
        diffs << "#{path}.#{key}: missing from astra"
      elsif !os_node.key?(key)
        diffs << "#{path}.#{key}: missing from os"
      elsif astra_node[key].is_a?(Hash) || os_node[key].is_a?(Hash) ||
          astra_node[key].is_a?(Array) || os_node[key].is_a?(Array)
        compare_agg_node(astra_node[key], os_node[key], "#{path}.#{key}", diffs)
      else
        compare_scalar(astra_node[key], os_node[key], "#{path}.#{key}", diffs)
      end
      return if diffs.any?
    end
    return
  end

  if astra_node.is_a?(Array) && os_node.is_a?(Array)
    if astra_node.size != os_node.size
      diffs << "#{path} size: astra=#{astra_node.size} os=#{os_node.size}"
      return
    end
    astra_node.zip(os_node).each_with_index do |(left, right), index|
      compare_agg_node(left, right, "#{path}[#{index}]", diffs)
      return if diffs.any?
    end
    return
  end

  compare_scalar(astra_node, os_node, path, diffs)
end

def sort_fields(request)
  Array(request["sort"]).flat_map do |sort_spec|
    sort_spec.is_a?(Hash) ? sort_spec.keys : []
  end
end

def compare_hits(astra_resp, os_resp, request, diffs)
  if request_tracks_total_hits?(request)
    astra_total = astra_resp.dig("hits", "total", "value")
    os_total = os_resp.dig("hits", "total", "value")
    compare_scalar(astra_total, os_total, "hits.total.value", diffs)
    return if diffs.any?
  end

  astra_hits = astra_resp.dig("hits", "hits") || []
  os_hits = os_resp.dig("hits", "hits") || []
  if astra_hits.size != os_hits.size
    diffs << "hits.hits size: astra=#{astra_hits.size} os=#{os_hits.size}"
    return
  end

  fields = sort_fields(request)
  return if fields.empty?

  astra_hits.zip(os_hits).each_with_index do |(astra_hit, os_hit), index|
    if astra_hit.key?("sort") && os_hit.key?("sort")
      astra_sort = astra_hit["sort"] || []
      os_sort = os_hit["sort"] || []
      if astra_sort.size != os_sort.size
        diffs << "hits.hits[#{index}].sort size: astra=#{astra_sort.size} os=#{os_sort.size}"
        return
      end
      astra_sort.zip(os_sort).each_with_index do |(left, right), sort_index|
        field = fields[sort_index] || sort_index
        compare_sort_scalar(left, right, "hits.hits[#{index}].sort[#{field}]", diffs)
        return if diffs.any?
      end
    else
      fields.each do |field|
        compare_scalar(
          astra_hit.dig("_source", field),
          os_hit.dig("_source", field),
          "hits.hits[#{index}]._source.#{field}",
          diffs)
        return if diffs.any?
      end
    end
  end
end

def compare_responses(astra_json, os_json, request)
  diffs = []
  astra_resp = response(astra_json)
  os_resp = response(os_json)
  return ["comparison skipped (missing msearch response)"] unless astra_resp && os_resp

  compare_hits(astra_resp, os_resp, request, diffs)
  return diffs if diffs.any?

  astra_aggs = astra_resp["aggregations"]
  os_aggs = os_resp["aggregations"]
  if astra_aggs || os_aggs
    if astra_aggs.nil? || os_aggs.nil?
      diffs << "aggregations: #{astra_aggs.nil? ? 'missing from astra' : 'missing from os'}"
    else
      compare_agg_node(astra_aggs, os_aggs, "aggregations", diffs)
    end
  end

  diffs
end

def msearch_body(index_name, request, subject, os_request_cache)
  selected_index = subject == "astra" ? ENV.fetch("ASTRA_INDEX_NAME", index_name) : index_name
  metadata = { "index" => selected_index }
  metadata["request_cache"] = os_request_cache if subject == "os"
  "#{JSON.generate(metadata)}\n#{JSON.generate(request)}\n"
end

def visible_doc_count(subject, urls, index_name, os_request_cache, os_user, os_pw, os_insecure)
  request = { "size" => 0, "track_total_hits" => true }
  status_code, raw_out =
    if subject == "os"
      post_json(
        urls.fetch(subject),
        msearch_body(index_name, request, subject, os_request_cache),
        user: os_user,
        password: os_pw,
        insecure: os_insecure,
        read_timeout: 30)
    else
      post_json(
        urls.fetch(subject),
        msearch_body(index_name, request, subject, os_request_cache),
        read_timeout: 30)
    end
  return 0 if status_code >= 300

  parsed = JSON.parse(raw_out)
  response(parsed)&.dig("hits", "total", "value").to_i
rescue StandardError
  0
end

def wait_for_visible_docs(
  subject,
  urls,
  index_name,
  expected_docs,
  os_request_cache,
  os_user,
  os_pw,
  os_insecure,
  timeout_seconds: 600)
  return if expected_docs <= 0

  label = subject == "astra" ? "KalDB" : "OpenSearch"
  deadline = Time.now + timeout_seconds
  puts "Waiting for #{label} to expose #{expected_docs} documents."
  loop do
    count = visible_doc_count(subject, urls, index_name, os_request_cache, os_user, os_pw, os_insecure)
    puts "#{label} visible documents: #{count}/#{expected_docs}"
    return if count >= expected_docs

    abort("Timed out waiting for #{label} visible documents") if Time.now >= deadline

    sleep 5
  end
end

def find_q19_user_id(urls, index_name, os_user, os_pw, os_insecure, os_request_cache)
  request = {
    "size" => 1,
    "_source" => ["UserID"],
    "query" => { "exists" => { "field" => "UserID" } }
  }
  status_code, raw_out =
    post_json(
      urls.fetch("os"),
      msearch_body(index_name, request, "os", os_request_cache),
      user: os_user,
      password: os_pw,
      insecure: os_insecure)
  abort("Q19 UserID lookup failed with HTTP #{status_code}: #{raw_out}") if status_code >= 300

  parsed = JSON.parse(raw_out)
  response_error = response(parsed)&.fetch("error", nil)
  abort("Q19 UserID lookup returned response error: #{response_error}") if response_error

  user_id = response(parsed)&.dig("hits", "hits", 0, "_source", "UserID")
  abort("Q19 UserID lookup did not return a UserID") if user_id.nil?

  user_id
rescue JSON::ParserError => e
  abort("Q19 UserID lookup returned invalid JSON: #{e.message}")
end

def patch_q19_user_id!(queries, urls, index_name, os_user, os_pw, os_insecure, os_request_cache)
  return if ENV.fetch("CB_PATCH_Q19_USER_ID", "true") == "false"

  q19 = queries.find { |query| query[:name] == "q19" }
  return unless q19

  term = q19[:request].dig("query", "term")
  abort("Q19 request shape changed; expected query.term.UserID") unless term.is_a?(Hash) && term.key?("UserID")

  user_id =
    if ENV["CB_Q19_USER_ID"] && !ENV["CB_Q19_USER_ID"].empty?
      Integer(ENV.fetch("CB_Q19_USER_ID"))
    else
      find_q19_user_id(urls, index_name, os_user, os_pw, os_insecure, os_request_cache)
    end
  term["UserID"] = user_id
  puts "Q19 benchmark UserID: #{user_id}"
end

def run_query(subject, url, index_name, request, output_file, os_request_cache, os_user, os_pw, os_insecure)
  raw_out = nil
  status_code = nil
  elapsed = Benchmark.realtime do
    status_code, raw_out =
      if subject == "os"
        post_json(
          url,
          msearch_body(index_name, request, subject, os_request_cache),
          user: os_user,
          password: os_pw,
          insecure: os_insecure)
      else
        post_json(url, msearch_body(index_name, request, subject, os_request_cache))
      end
  end

  File.write(output_file, raw_out)
  parsed = begin
    JSON.parse(raw_out)
  rescue JSON::ParserError => e
    return {
      elapsed_ms: elapsed * 1000,
      failed: true,
      failure: "parse error: #{e.message}",
      parsed: nil
    }
  end

  failure_messages = []
  failure_messages << "HTTP #{status_code}" if status_code >= 300
  top_level_error = parsed&.fetch("error", nil)
  failure_messages << "top-level error: #{top_level_error}" if top_level_error
  failure_messages << "msearch errors=true" if parsed&.fetch("errors", false)
  response_status = response(parsed)&.fetch("status", nil)
  failure_messages << "response status: #{response_status}" if response_status && response_status.to_i >= 300
  response_error = response(parsed)&.fetch("error", nil)
  failure_messages << "response error: #{response_error}" if response_error
  failure_messages.concat(response_data_notes(subject, response(parsed), request)) if failure_messages.empty?

  {
    elapsed_ms: elapsed * 1000,
    failed: failure_messages.any?,
    failure: failure_messages.join("; "),
    parsed: parsed
  }
end

def drop_host_caches
  shell!("sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'")
end

def clear_opensearch_caches
  shell("curl -fsS -XPOST 'http://localhost:9200/hits/_cache/clear?request=true&query=true&fielddata=true' >/dev/null")
end

def start_or_restart_container(container_name)
  running =
    `docker inspect -f '{{.State.Running}}' #{container_name} 2>/dev/null`.strip == "true"
  if running
    shell!("docker restart #{container_name} >/dev/null")
  else
    shell!("docker start #{container_name} >/dev/null")
  end
end

def restart_subject(
  subject,
  isolate_subjects,
  urls,
  index_name,
  expected_docs,
  os_request_cache,
  os_user,
  os_pw,
  os_insecure)
  case subject
  when "astra"
    shell("docker stop dep_opensearch >/dev/null 2>&1") if isolate_subjects
    shell!("docker start dep_zookeeper dep_kafka dep_s3 dep_openzipkin >/dev/null")
    start_or_restart_container("astra_single")
    wait_for_url("KalDB query", "http://localhost:8081/metrics")
    wait_for_visible_docs(
      subject,
      urls,
      index_name,
      expected_docs,
      os_request_cache,
      os_user,
      os_pw,
      os_insecure)
    drop_host_caches
  when "os"
    shell("docker stop astra_single >/dev/null 2>&1") if isolate_subjects
    start_or_restart_container("dep_opensearch")
    wait_for_url("OpenSearch", "http://localhost:9200/_cluster/health", timeout_seconds: 360)
    clear_opensearch_caches
    wait_for_visible_docs(
      subject,
      urls,
      index_name,
      expected_docs,
      os_request_cache,
      os_user,
      os_pw,
      os_insecure)
    drop_host_caches
  else
    abort("Unknown subject: #{subject}")
  end
end

def ensure_services_for_setup(subjects)
  shell!("docker start dep_zookeeper dep_kafka dep_s3 dep_openzipkin >/dev/null")
  if subjects.include?("os")
    shell!("docker start dep_opensearch >/dev/null")
    wait_for_url("OpenSearch", "http://localhost:9200/_cluster/health", timeout_seconds: 360)
  end
  if subjects.include?("astra")
    shell!("docker start astra_single >/dev/null")
    wait_for_url("KalDB query", "http://localhost:8081/metrics")
  end
end

def write_summary_csv(path, sample_rows)
  grouped = sample_rows.group_by { |row| [row[:repetition], row[:subject], row[:query]] }
  CSV.open(path, "w") do |csv|
    csv << [
      "repetition",
      "subject",
      "query",
      "cold_ms",
      "hot1_ms",
      "hot2_ms",
      "hot_min_ms",
      "failed",
      "failure"
    ]
    grouped.keys.sort_by { |repetition, subject, query| [repetition, subject, query_number(query)] }.each do |key|
      rows = grouped.fetch(key)
      by_phase = rows.to_h { |row| [row[:phase], row] }
      hot_values = [by_phase.dig("hot1", :elapsed_ms), by_phase.dig("hot2", :elapsed_ms)].compact
      failures = rows.select { |row| row[:failed] }
      csv << [
        key[0],
        key[1],
        key[2],
        by_phase.dig("cold", :elapsed_ms)&.round(3),
        by_phase.dig("hot1", :elapsed_ms)&.round(3),
        by_phase.dig("hot2", :elapsed_ms)&.round(3),
        hot_values.min&.round(3),
        failures.any?,
        failures.map { |row| row[:failure] }.reject(&:empty?).uniq.join("; ")
      ]
    end
  end
end

def query_number(query)
  match = query.to_s.match(/\Aq(\d+)\z/)
  match ? match[1].to_i : Float::INFINITY
end

def write_samples_csv(path, sample_rows)
  CSV.open(path, "w") do |csv|
    csv << ["repetition", "subject", "query", "phase", "elapsed_ms", "failed", "failure"]
    sample_rows.each do |row|
      csv << [
        row[:repetition],
        row[:subject],
        row[:query],
        row[:phase],
        row[:elapsed_ms].round(3),
        row[:failed],
        row[:failure]
      ]
    end
  end
end

def write_response_comparison_csv(path, response_rows)
  CSV.open(path, "w") do |csv|
    csv << ["repetition", "query", "phase", "notes"]
    response_rows.each do |row|
      csv << [row[:repetition], row[:query], row[:phase], row[:notes]]
    end
  end
end

def pct_delta(left, right)
  return nil if left.nil? || right.nil? || left.to_f.zero?

  ((right.to_f - left.to_f) / left.to_f * 100.0)
end

def write_variation_csv(path, summary_csv_path)
  rows = CSV.read(summary_csv_path, headers: true)
  grouped = rows.group_by { |row| [row["subject"], row["query"]] }
  CSV.open(path, "w") do |csv|
    csv << [
      "subject",
      "query",
      "run1_cold_ms",
      "run2_cold_ms",
      "cold_delta_pct",
      "run1_hot_min_ms",
      "run2_hot_min_ms",
      "hot_min_delta_pct"
    ]
    grouped.keys.sort_by { |subject, query| [subject, query_number(query)] }.each do |subject, query|
      by_rep = grouped.fetch([subject, query]).to_h { |row| [row["repetition"].to_i, row] }
      next unless by_rep[1] && by_rep[2]

      run1_cold = by_rep[1]["cold_ms"].to_f
      run2_cold = by_rep[2]["cold_ms"].to_f
      run1_hot = by_rep[1]["hot_min_ms"].to_f
      run2_hot = by_rep[2]["hot_min_ms"].to_f
      csv << [
        subject,
        query,
        format("%.3f", run1_cold),
        format("%.3f", run2_cold),
        format("%+.2f", pct_delta(run1_cold, run2_cold)),
        format("%.3f", run1_hot),
        format("%.3f", run2_hot),
        format("%+.2f", pct_delta(run1_hot, run2_hot))
      ]
    end
  end
end

options = {
  repetitions: 2,
  label: "clickbench-protocol",
  subjects: ENV.fetch("PROTOCOL_SUBJECTS", "astra,os").split(",").map(&:strip).reject(&:empty?),
  isolate_subjects: boolean_env("PROTOCOL_ISOLATE_SUBJECTS", "true")
}

OptionParser.new do |parser|
  parser.on("--repetitions N", Integer) { |value| options[:repetitions] = value }
  parser.on("--label LABEL") { |value| options[:label] = value }
  parser.on("--subjects LIST") do |value|
    options[:subjects] = value.split(",").map(&:strip).reject(&:empty?)
  end
end.parse!

abort("--repetitions must be positive") if options[:repetitions] <= 0

index_name = ENV.fetch("INDEX_NAME", "hits")
os_scheme = ENV.fetch("OS_SCHEME", "http")
os_host = ENV.fetch("OS_HOST", "localhost")
os_port = ENV.fetch("OS_PORT", "9200")
os_user = ENV.fetch("OS_USER", "")
os_pw = ENV.fetch("OS_PW", "")
os_insecure = ENV.fetch("OS_CURL_INSECURE", "false") == "true"
os_request_cache = boolean_env("OS_REQUEST_CACHE", "false")
measured_track_total_hits = boolean_env("CLICKBENCH_TRACK_TOTAL_HITS", "false")
expected_docs = Integer(ENV.fetch("EXPECTED_DOCS", "0"))
astra_host = ENV.fetch("ASTRA_QUERY_HOST", "localhost")
astra_port = ENV.fetch("ASTRA_QUERY_PORT", "8081")
urls = {
  "astra" => "http://#{astra_host}:#{astra_port}/_msearch",
  "os" => "#{os_scheme}://#{os_host}:#{os_port}/_msearch"
}

ensure_services_for_setup(options[:subjects])

queries = ClickBenchQueries.load
if ENV["BENCHMARK_QUERIES"] && !ENV["BENCHMARK_QUERIES"].empty?
  selected_queries = ENV["BENCHMARK_QUERIES"].split(",").map { |name| name.strip.downcase }
  queries = queries.select do |query|
    selected_queries.include?(query[:name]) || selected_queries.include?("q#{query[:query_number]}")
  end
  missing_queries = selected_queries - queries.map { |query| query[:name] }
  abort("Unknown BENCHMARK_QUERIES: #{missing_queries.join(", ")}") unless missing_queries.empty?
end
patch_q19_user_id!(queries, urls, index_name, os_user, os_pw, os_insecure, os_request_cache)

run_id = Time.now.utc.strftime("%Y-%m-%d-%H-%M-%S")
run_dir = File.join(SCRIPT_DIR, "history", "protocol-runs", "#{run_id}-#{options[:label]}")
output_dir = File.join(run_dir, "output")
FileUtils.mkdir_p(output_dir)

metadata = {
  started_at: Time.now.utc.iso8601,
  label: options[:label],
  repetitions: options[:repetitions],
  subjects: options[:subjects],
  isolate_subjects: options[:isolate_subjects],
  query_profile: ClickBenchQueries.default_profile,
  os_request_cache: os_request_cache,
  measured_track_total_hits: measured_track_total_hits,
  expected_docs: expected_docs,
  git: {
    branch: run_cmd("git", "branch", "--show-current")[:stdout].strip,
    head: run_cmd("git", "rev-parse", "HEAD")[:stdout].strip,
    status_short: run_cmd("git", "status", "--short")[:stdout]
  },
  env: {
    "ASTRA_INDEX_NAME" => ENV["ASTRA_INDEX_NAME"],
    "OPENSEARCH_JAVA_OPTS" => ENV["OPENSEARCH_JAVA_OPTS"],
    "CLICKBENCH_TRACK_TOTAL_HITS" => ENV["CLICKBENCH_TRACK_TOTAL_HITS"],
    "OS_NUMBER_OF_SHARDS" => ENV["OS_NUMBER_OF_SHARDS"],
    "EXPECTED_DOCS" => ENV["EXPECTED_DOCS"],
    "INDEXER_MAX_MESSAGES_PER_CHUNK" => ENV["INDEXER_MAX_MESSAGES_PER_CHUNK"],
    "INDEXER_MAX_BYTES_PER_CHUNK" => ENV["INDEXER_MAX_BYTES_PER_CHUNK"],
    "ASTRA_PARTITION_MAX_CAPACITY_BYTES" => ENV["ASTRA_PARTITION_MAX_CAPACITY_BYTES"]
  },
  jvm: JvmCapture.write(run_dir, subjects: options[:subjects])
}
File.write(File.join(run_dir, "run.json"), JSON.pretty_generate(metadata) + "\n")

sample_rows = []
response_rows = []
parsed_outputs = {}
samples_csv = File.join(run_dir, "samples.csv")
response_comparison_csv = File.join(run_dir, "response_comparison.csv")

queries.each_with_index do |query, query_index|
  query_name = query[:name]
  puts
  puts "=" * 80
  puts "#{query_name.upcase} #{query_index + 1}/#{queries.size}"
  puts "=" * 80

  options[:repetitions].times do |rep_index|
    repetition = rep_index + 1
    puts
    puts "-" * 80
    puts "Protocol repetition #{repetition}/#{options[:repetitions]}"
    puts "-" * 80

    options[:subjects].each do |subject|
      request = benchmark_request(query[:request], measured_track_total_hits)

      puts
      puts "#{query_name.upcase} #{subject}: cold setup"
      restart_subject(
        subject,
        options[:isolate_subjects],
        urls,
        index_name,
        expected_docs,
        os_request_cache,
        os_user,
        os_pw,
        os_insecure)

      PHASES.each do |phase|
        output_file = File.join(output_dir, "rep#{repetition}-#{subject}-#{query_name}-#{phase}.json")
        result =
          run_query(
            subject,
            urls.fetch(subject),
            index_name,
            request,
            output_file,
            os_request_cache,
            os_user,
            os_pw,
            os_insecure)
        parsed_outputs[[repetition, subject, query_name, phase]] = result[:parsed]
        sample_rows << {
          repetition: repetition,
          subject: subject,
          query: query_name,
          phase: phase,
          elapsed_ms: result[:elapsed_ms],
          failed: result[:failed],
          failure: result[:failure]
        }
        marker = result[:failed] ? "* #{result[:failure]}" : ""
        puts "#{subject} #{query_name} #{phase}: #{result[:elapsed_ms].round} ms #{marker}"
        write_samples_csv(samples_csv, sample_rows)
      end
    end

    PHASES.each do |phase|
      astra_json = parsed_outputs[[repetition, "astra", query_name, phase]]
      os_json = parsed_outputs[[repetition, "os", query_name, phase]]
      next unless astra_json && os_json

      notes = compare_responses(
        astra_json,
        os_json,
        benchmark_request(query[:request], measured_track_total_hits))
      response_rows << {
        repetition: repetition,
        query: query_name,
        phase: phase,
        notes: notes
      }
    end
    write_response_comparison_csv(response_comparison_csv, response_rows)
  end
end

summary_csv = File.join(run_dir, "summary.csv")
write_summary_csv(summary_csv, sample_rows)
write_variation_csv(File.join(run_dir, "variation.csv"), summary_csv) if options[:repetitions] >= 2

metadata[:finished_at] = Time.now.utc.iso8601
metadata[:failed_sample_rows] = sample_rows.count { |row| row[:failed] }
File.write(File.join(run_dir, "run.json"), JSON.pretty_generate(metadata) + "\n")

ensure_services_for_setup(options[:subjects])

puts
puts "Recorded ClickBench protocol bundle at #{run_dir}"
exit(sample_rows.any? { |row| row[:failed] } ? 1 : 0)
