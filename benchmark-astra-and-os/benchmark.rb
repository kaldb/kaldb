#!/usr/bin/env ruby
require 'benchmark'
require 'csv'
require 'json'
require 'net/http'
require 'openssl'
require 'shellwords'
require 'time'
require 'uri'

if ENV.fetch("DATASET", "clickbench_hits") == "clickbench_hits"
  require_relative "clickbench_queries"

  def cb_post_json(url, body, user, password, insecure)
    uri = URI(url)
    request = Net::HTTP::Post.new(uri)
    request["Content-Type"] = "application/json"
    request.body = body
    request.basic_auth(user.empty? ? "admin" : user, password) if !user.empty? || !password.empty?

    http = Net::HTTP.new(uri.host, uri.port)
    http.open_timeout = Integer(ENV.fetch("BENCHMARK_OPEN_TIMEOUT_SECONDS", "10"))
    http.read_timeout = Integer(ENV.fetch("BENCHMARK_READ_TIMEOUT_SECONDS", "600"))
    if uri.scheme == "https"
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE if insecure
    end

    response = http.request(request)
    [response.code.to_i, response.body]
  end

  def cb_response(json)
    json&.dig("responses", 0)
  end

  def cb_request_has_aggregations?(request)
    request.key?("aggs") || request.key?("aggregations")
  end

  def cb_request_tracks_total_hits?(request)
    value = request.fetch("track_total_hits", true)
    value != false && value != -1
  end

  def cb_response_data_notes(subject, response, request)
    notes = []
    unless response
      notes << "#{subject}: missing msearch response"
      return notes
    end

    hits = response["hits"]
    total = hits&.dig("total", "value")
    returned_hits = hits&.fetch("hits", nil)
    requested_size = Integer(request.fetch("size", 10))

    if cb_request_tracks_total_hits?(request)
      if total.nil?
        notes << "#{subject}: missing hits.total.value"
      elsif total.to_i <= 0
        notes << "#{subject}: hits.total.value is #{total}"
      end
    end

    if cb_request_has_aggregations?(request)
      aggregations = response["aggregations"]
      notes << "#{subject}: missing aggregations" unless aggregations.is_a?(Hash) && !aggregations.empty?
    elsif requested_size.positive?
      notes << "#{subject}: no returned hits" unless returned_hits.is_a?(Array) && !returned_hits.empty?
    end

    notes
  end

  def cb_numeric_equal?(left, right)
    return true if left == right
    return false unless left.is_a?(Numeric) && right.is_a?(Numeric)

    (left.to_f - right.to_f).abs <= [0.001, right.to_f.abs * 0.000001].max
  end

  def cb_compare_scalar(left, right, path, diffs)
    if left.is_a?(Numeric) || right.is_a?(Numeric)
      diffs << "#{path}: astra=#{left.inspect} os=#{right.inspect}" unless cb_numeric_equal?(left, right)
      return
    end

    diffs << "#{path}: astra=#{left.inspect} os=#{right.inspect}" unless left == right
  end

  def cb_iso8601_millis(value)
    return nil unless value.is_a?(String)

    (Time.iso8601(value).to_r * 1000).round
  rescue ArgumentError
    nil
  end

  def cb_compare_sort_scalar(left, right, path, diffs)
    left_millis = cb_iso8601_millis(left)
    right_millis = cb_iso8601_millis(right)
    if left_millis && right.is_a?(Numeric)
      cb_compare_scalar(left_millis, right, path, diffs)
    elsif right_millis && left.is_a?(Numeric)
      cb_compare_scalar(left, right_millis, path, diffs)
    else
      cb_compare_scalar(left, right, path, diffs)
    end
  end

  CB_IGNORED_AGG_KEYS = [
    "doc_count_error_upper_bound",
    "sum_other_doc_count",
    "key_as_string",
    "value_as_string"
  ].freeze

  def cb_compare_agg_node(astra_node, os_node, path, diffs)
    if astra_node.is_a?(Hash) && os_node.is_a?(Hash)
      if astra_node.key?("value") || os_node.key?("value")
        cb_compare_scalar(astra_node["value"], os_node["value"], "#{path}.value", diffs)
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
          cb_compare_agg_node(astra_bucket, os_bucket, "#{path}.buckets[#{index}]", diffs)
          return if diffs.any?
        end
      end

      keys = (astra_node.keys | os_node.keys) - CB_IGNORED_AGG_KEYS - ["buckets", "value"]
      keys.sort.each do |key|
        if !astra_node.key?(key)
          diffs << "#{path}.#{key}: missing from astra"
        elsif !os_node.key?(key)
          diffs << "#{path}.#{key}: missing from os"
        elsif astra_node[key].is_a?(Hash) || os_node[key].is_a?(Hash) ||
            astra_node[key].is_a?(Array) || os_node[key].is_a?(Array)
          cb_compare_agg_node(astra_node[key], os_node[key], "#{path}.#{key}", diffs)
        else
          cb_compare_scalar(astra_node[key], os_node[key], "#{path}.#{key}", diffs)
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
        cb_compare_agg_node(left, right, "#{path}[#{index}]", diffs)
        return if diffs.any?
      end
      return
    end

    cb_compare_scalar(astra_node, os_node, path, diffs)
  end

  def cb_sort_fields(request)
    Array(request["sort"]).flat_map do |sort_spec|
      sort_spec.is_a?(Hash) ? sort_spec.keys : []
    end
  end

  def cb_compare_hits(astra_resp, os_resp, request, diffs)
    if cb_request_tracks_total_hits?(request)
      astra_total = astra_resp.dig("hits", "total", "value")
      os_total = os_resp.dig("hits", "total", "value")
      cb_compare_scalar(astra_total, os_total, "hits.total.value", diffs)
      return if diffs.any?
    end

    astra_hits = astra_resp.dig("hits", "hits") || []
    os_hits = os_resp.dig("hits", "hits") || []
    if astra_hits.size != os_hits.size
      diffs << "hits.hits size: astra=#{astra_hits.size} os=#{os_hits.size}"
      return
    end

    sort_fields = cb_sort_fields(request)
    return if sort_fields.empty?

    astra_hits.zip(os_hits).each_with_index do |(astra_hit, os_hit), index|
      if astra_hit.key?("sort") && os_hit.key?("sort")
        astra_sort = astra_hit["sort"] || []
        os_sort = os_hit["sort"] || []
        if astra_sort.size != os_sort.size
          diffs << "hits.hits[#{index}].sort size: astra=#{astra_sort.size} os=#{os_sort.size}"
          return
        end
        astra_sort.zip(os_sort).each_with_index do |(left, right), sort_index|
          field = sort_fields[sort_index] || sort_index
          cb_compare_sort_scalar(left, right, "hits.hits[#{index}].sort[#{field}]", diffs)
          return if diffs.any?
        end
      else
        sort_fields.each do |field|
          cb_compare_scalar(
            astra_hit.dig("_source", field),
            os_hit.dig("_source", field),
            "hits.hits[#{index}]._source.#{field}",
            diffs)
          return if diffs.any?
        end
      end
      return if diffs.any?
    end
  end

  def cb_compare_responses(astra_json, os_json, request)
    diffs = []
    astra_resp = cb_response(astra_json)
    os_resp = cb_response(os_json)
    unless astra_resp && os_resp
      return ["comparison skipped (missing msearch response)"]
    end

    cb_compare_hits(astra_resp, os_resp, request, diffs)
    return diffs if diffs.any?

    astra_aggs = astra_resp["aggregations"]
    os_aggs = os_resp["aggregations"]
    if astra_aggs || os_aggs
      if astra_aggs.nil? || os_aggs.nil?
        diffs << "aggregations: #{astra_aggs.nil? ? 'missing from astra' : 'missing from os'}"
      else
        cb_compare_agg_node(astra_aggs, os_aggs, "aggregations", diffs)
      end
    end

    diffs
  end

  def cb_expected_notes?(notes)
    notes.to_a.any? { |note| note.to_s.start_with?("expected:") }
  end

  def cb_boolean_env(name, default)
    case ENV.fetch(name, default).downcase
    when "true"
      true
    when "false"
      false
    else
      abort("#{name} must be true or false")
    end
  end

  def cb_msearch_body(index_name, request, subject, os_request_cache)
    selected_index = subject == "astra" ? ENV.fetch("ASTRA_INDEX_NAME", index_name) : index_name
    metadata = { "index" => selected_index }
    metadata["request_cache"] = os_request_cache if subject == "os"
    "#{JSON.generate(metadata)}\n#{JSON.generate(request)}\n"
  end

  def cb_find_q19_user_id(urls, index_name, os_user, os_pw, os_insecure, os_request_cache)
    request = {
      "size" => 1,
      "_source" => ["UserID"],
      "query" => { "exists" => { "field" => "UserID" } }
    }
    status_code, raw_out =
      cb_post_json(
        urls.fetch("os"),
        cb_msearch_body(index_name, request, "os", os_request_cache),
        os_user,
        os_pw,
        os_insecure)
    abort("Q19 UserID lookup failed with HTTP #{status_code}: #{raw_out}") if status_code >= 300

    parsed = JSON.parse(raw_out)
    response = cb_response(parsed)
    response_error = response&.fetch("error", nil)
    abort("Q19 UserID lookup returned response error: #{response_error}") if response_error

    user_id = response&.dig("hits", "hits", 0, "_source", "UserID")
    abort("Q19 UserID lookup did not return a UserID") if user_id.nil?

    user_id
  rescue JSON::ParserError => e
    abort("Q19 UserID lookup returned invalid JSON: #{e.message}")
  end

  def cb_patch_q19_user_id!(queries, urls, index_name, os_user, os_pw, os_insecure, os_request_cache)
    return if ENV.fetch("CB_PATCH_Q19_USER_ID", "true") == "false"

    q19 = queries.find { |query| query[:name] == "q19" }
    return unless q19

    term = q19[:request].dig("query", "term")
    abort("Q19 request shape changed; expected query.term.UserID") unless term.is_a?(Hash) && term.key?("UserID")

    user_id =
      if ENV["CB_Q19_USER_ID"] && !ENV["CB_Q19_USER_ID"].empty?
        Integer(ENV.fetch("CB_Q19_USER_ID"))
      else
        cb_find_q19_user_id(urls, index_name, os_user, os_pw, os_insecure, os_request_cache)
      end
    term["UserID"] = user_id
    puts "Q19 benchmark UserID: #{user_id}"
  end

  def cb_median(values)
    sorted = values.sort
    midpoint = sorted.size / 2
    return sorted[midpoint] if sorted.size.odd?

    (sorted[midpoint - 1] + sorted[midpoint]) / 2.0
  end

  def cb_trimmed_average(values)
    trimmed = values.sort
    trimmed = trimmed[1...-1] if trimmed.size >= 3
    trimmed.sum / trimmed.size
  end

  def cb_latency_stats(values)
    {
      median: cb_median(values),
      trimmed_avg: cb_trimmed_average(values),
      min: values.min,
      max: values.max,
      samples: values.size
    }
  end

  def cb_aggregate_rows(sample_rows)
    grouped = sample_rows.group_by { |row| [row[:name], row[:count]] }
    grouped.keys.sort_by { |name, count| [name.delete_prefix("q").to_i, count] }.map do |name, count|
      samples = grouped.fetch([name, count])
      astra_stats = cb_latency_stats(samples.map { |sample| sample[:astra_ms] })
      os_stats = cb_latency_stats(samples.map { |sample| sample[:os_ms] })
      astra_failures = samples.select { |sample| sample[:astra_failed] }
      os_failures = samples.select { |sample| sample[:os_failed] }
      comparison_notes = samples.flat_map { |sample| sample[:comparison_notes] }.uniq

      [
        name,
        count,
        astra_stats.fetch(:median).round(3),
        astra_stats.fetch(:median).round(3),
        astra_stats.fetch(:trimmed_avg).round(3),
        astra_stats.fetch(:min).round(3),
        astra_stats.fetch(:max).round(3),
        astra_stats.fetch(:samples),
        astra_failures.any?,
        astra_failures.map { |sample| sample[:astra_failure] }.reject(&:empty?).uniq.join("; "),
        os_stats.fetch(:median).round(3),
        os_stats.fetch(:median).round(3),
        os_stats.fetch(:trimmed_avg).round(3),
        os_stats.fetch(:min).round(3),
        os_stats.fetch(:max).round(3),
        os_stats.fetch(:samples),
        os_failures.any?,
        os_failures.map { |sample| sample[:os_failure] }.reject(&:empty?).uniq.join("; "),
        comparison_notes
      ]
    end
  end

  def cb_color_code(ratio, mismatch: false)
    return "\033[97;90m!#{ratio.round(2).to_s.ljust(4)}\033[0m" if mismatch

    num = if ratio == 1
            40
          elsif ratio > 1.5
            101
          elsif ratio > 1
            41
          elsif ratio < 0.5
            102
          elsif ratio < 1
            42
          else
            105
          end
    "\033[97;#{num}m #{ratio.round(2).to_s.ljust(4)}\033[0m"
  end

  index_name = ENV.fetch("INDEX_NAME", "hits")
  ports = { "astra" => ENV.fetch("ASTRA_QUERY_PORT", "8081").to_i,
            "os" => ENV.fetch("OS_PORT", "9200").to_i }
  hosts = { "astra" => ENV.fetch("ASTRA_QUERY_HOST", "localhost"),
            "os" => ENV.fetch("OS_HOST", "localhost") }
  os_scheme = ENV.fetch("OS_SCHEME", "http")
  os_user = ENV.fetch("OS_USER", "")
  os_pw = ENV.fetch("OS_PW", "")
  os_insecure = ENV.fetch("OS_CURL_INSECURE", "false") == "true"
  os_request_cache = cb_boolean_env("OS_REQUEST_CACHE", "false")
  urls = {
    "astra" => "http://#{hosts["astra"]}:#{ports["astra"]}/_msearch",
    "os" => "#{os_scheme}://#{hosts["os"]}:#{ports["os"]}/_msearch"
  }

  queries = ClickBenchQueries.load
  if ENV["BENCHMARK_QUERIES"] && !ENV["BENCHMARK_QUERIES"].empty?
    selected_queries = ENV["BENCHMARK_QUERIES"].split(",").map { |name| name.strip.downcase }
    queries = queries.select do |query|
      selected_queries.include?(query[:name]) || selected_queries.include?("q#{query[:query_number]}")
    end
    missing_queries = selected_queries - queries.map { |query| query[:name] }
    abort("Unknown BENCHMARK_QUERIES: #{missing_queries.join(", ")}") unless missing_queries.empty?
  end
  cb_patch_q19_user_id!(queries, urls, index_name, os_user, os_pw, os_insecure, os_request_cache)

  current_time = Time.now
  run_id = ENV.fetch("BENCHMARK_RUN_ID", current_time.strftime("%Y-%m-%d-%H-%M-%S"))
  output_root = ENV.fetch("BENCHMARK_OUTPUT_ROOT", "output")
  results_root = ENV.fetch("BENCHMARK_RESULTS_ROOT", "results")
  results_prefix = ENV.fetch("BENCHMARK_RESULTS_PREFIX", "benchmark")
  output_dir = File.join(output_root, run_id)
  results_file = ENV.fetch("BENCHMARK_RESULTS_FILE", File.join(results_root, "#{results_prefix}.#{run_id}.csv"))
  samples_file = ENV.fetch("BENCHMARK_SAMPLES_FILE", File.join(results_root, "#{results_prefix}.#{run_id}.samples.csv"))

  Dir.mkdir(output_root) unless Dir.exist?(output_root)
  Dir.mkdir(output_dir) unless Dir.exist?(output_dir)

  sample_rows = []
  subjects = ["astra", "os"]
  iterations = (ARGV.first || 1).to_i
  iterations = 1 if iterations <= 0
  track_total_hits = cb_boolean_env("CLICKBENCH_TRACK_TOTAL_HITS", "false")

  iterations.times do |iteration|
    puts
    puts
    puts "ClickBench iteration #{iteration + 1}/#{iterations} at #{current_time.strftime "%Y-%m-%d %H:%M:%S"}"
    puts "=" * 80

    queries.each_with_index do |query, index|
      request = Marshal.load(Marshal.dump(query[:request]))
      request["track_total_hits"] = track_total_hits
      request_size = Integer(request.fetch("size", 10))
      puts
      puts "=" * 80
      puts "#{(index + 1).to_s.rjust(2)}/#{queries.size} #{query[:name].upcase}"
      puts "=" * 80

      timings = subjects.map do |subject|
        raw_out = nil
        status_code = nil
        failed = false
        fail_messages = []
        elapsed = Benchmark.realtime do
          request_body = cb_msearch_body(index_name, request, subject, os_request_cache)
          status_code, raw_out =
            if subject == "os"
              cb_post_json(urls.fetch(subject), request_body, os_user, os_pw, os_insecure)
            else
              cb_post_json(urls.fetch(subject), request_body, "", "", false)
            end
        end

        File.write(File.join(output_dir, "#{query[:name]}-iter#{iteration + 1}-#{subject}.json"), raw_out)
        parsed = begin
          JSON.parse(raw_out)
        rescue JSON::ParserError => e
          failed = true
          fail_messages << "parse error: #{e.message}"
          nil
        end

        failed = true if status_code >= 300
        fail_messages << "HTTP #{status_code}" if status_code >= 300
        top_level_error = parsed&.fetch("error", nil)
        if top_level_error
          failed = true
          fail_messages << "top-level error: #{top_level_error}"
        end
        if parsed&.fetch("errors", false)
          failed = true
          fail_messages << "msearch errors=true"
        end
        response_status = cb_response(parsed)&.fetch("status", nil)
        if response_status && response_status.to_i >= 300
          failed = true
          fail_messages << "response status: #{response_status}"
        end
        response_error = cb_response(parsed)&.fetch("error", nil)
        if response_error
          failed = true
          fail_messages << "response error: #{response_error}"
        end
        unless failed
          data_notes = cb_response_data_notes(subject, cb_response(parsed), request)
          if data_notes.any?
            failed = true
            fail_messages.concat(data_notes)
          end
        end

        [elapsed * 1000, failed, fail_messages.join("; "), parsed]
      end

      comparison_notes =
        if timings.any? { |timing| timing[1] }
          []
        else
          cb_compare_responses(timings[0][3], timings[1][3], request)
        end

      sample_rows << {
        name: query[:name],
        iteration: iteration + 1,
        count: request_size,
        astra_ms: timings[0][0],
        astra_failed: timings[0][1],
        astra_failure: timings[0][2],
        os_ms: timings[1][0],
        os_failed: timings[1][1],
        os_failure: timings[1][2],
        comparison_notes: comparison_notes
      }

      astra_cell = "#{timings[0][0].round.to_s.rjust(8)}#{timings[0][1] ? "* #{timings[0][2]}" : ""}"
      os_cell = "#{timings[1][0].round.to_s.rjust(8)}#{timings[1][1] ? "* #{timings[1][2]}" : ""}"
      notes = comparison_notes.empty? ? "" : " #{comparison_notes.join("; ")}"
      puts "size=#{request_size.to_s.ljust(5)} astra_ms=#{astra_cell} os_ms=#{os_cell}#{notes}"
    end
  end

  Dir.mkdir(results_root) unless Dir.exist?(results_root)
  CSV.open(samples_file, "w") do |csv|
    csv << [
      :name,
      :iteration,
      :count,
      :astra_ms,
      :astra_asterisk,
      :astra_asterisk_why,
      :os_ms,
      :os_asterisk,
      :os_asterisk_why,
      :comparison_notes
    ]
    sample_rows.each do |row|
      csv << [
        row[:name],
        row[:iteration],
        row[:count],
        row[:astra_ms].round(3),
        row[:astra_failed],
        row[:astra_failure],
        row[:os_ms].round(3),
        row[:os_failed],
        row[:os_failure],
        row[:comparison_notes]
      ]
    end
  end

  rows = [
    [
      :name,
      :count,
      :astra_ms,
      :astra_median_ms,
      :astra_trimmed_avg_ms,
      :astra_min_ms,
      :astra_max_ms,
      :astra_samples,
      :astra_asterisk,
      :astra_asterisk_why,
      :os_ms,
      :os_median_ms,
      :os_trimmed_avg_ms,
      :os_min_ms,
      :os_max_ms,
      :os_samples,
      :os_asterisk,
      :os_asterisk_why,
      :comparison_notes
    ],
    *cb_aggregate_rows(sample_rows)
  ]

  CSV.open(results_file, "w") do |csv|
    rows.each { |row| csv << row }
  end

  puts
  puts "=" * 80
  puts "ClickBench KalDB/OpenSearch median ratios"
  rows[1..].each do |row|
    name = row[0]
    astra_ms = row[2]
    os_ms = row[10]
    astra_failed = row[8]
    os_failed = row[16]
    notes = row[18]
    ratio = os_ms.positive? ? astra_ms / os_ms : 0
    mismatch = astra_failed || os_failed || (notes.any? && !cb_expected_notes?(notes))
    puts "#{name.ljust(8)} #{cb_color_code(ratio, mismatch: mismatch)}"
  end
  puts
  puts "Legend: ratio = median astra_ms/os_ms. >1 = OS faster, <1 = KalDB faster, ! = result mismatch or request failure"

  exit(rows[1..].any? { |row| row[8] || row[16] || row[18].any? } ? 1 : 0)
end

def stat_output count, timings, astrisk_when
  timings_component = timings.map { |t, o, output|
    astrisk, astrisk_why = astrisk_when[count, o]
    [(t.real*1000).to_i, astrisk, astrisk_why]
  }.flatten
  "#{count.to_s.ljust(46)}#{timings_component}"
  [count, *timings_component]
end

def output_line count, timings, astrisk_when
  timings_component = timings.map { |t, o, output|
    astrisk, astrisk_why = astrisk_when[count, o]
    "#{(t.real*1000).to_i.to_s.rjust(10)}#{ astrisk ? "* #{astrisk_why && astrisk_why.to_s.gsub("\n",'')}": '' }"
  }.join(' ')
  "#{count.to_s.ljust(46)}#{timings_component}"
end

def run_jq jq_query, out
  IO.popen jq_query, 'r+' do |io|
    io.puts out
    io.close_write
    io.gets || ''
  end.strip
end

# Compare responses between Astra and OpenSearch, aware of known format differences.
# Returns an array of mismatch descriptions (empty = all good).
def compare_responses(astra_json, os_json, query_str, requested_size)
  notes = []
  if astra_json.nil? || os_json.nil?
    notes << "comparison skipped (parse error)"
    return notes
  end

  astra_resp = astra_json.dig("responses", 0)
  os_resp = os_json.dig("responses", 0)
  unless astra_resp && os_resp
    notes << "comparison skipped (missing response)"
    return notes
  end

  has_aggs = query_str.include?('"aggs"')
  has_sort = query_str.include?('"sort"')

  # Compare hit counts (both should return the requested size, or fewer if not enough docs)
  astra_hit_ct = (astra_resp.dig("hits", "hits") || []).size
  os_hit_ct = (os_resp.dig("hits", "hits") || []).size
  if astra_hit_ct != os_hit_ct
    notes << "hit count: astra=#{astra_hit_ct} os=#{os_hit_ct}"
  end

  # Compare hit documents only when there's an explicit sort order.
  # Without explicit sort, Astra and OS return different documents (different default ordering).
  if has_sort && requested_size > 0
    astra_vals = (astra_resp.dig("hits", "hits") || []).map { |h| h.dig("_source", "total_amount") }
    os_vals = (os_resp.dig("hits", "hits") || []).map { |h| h.dig("_source", "total_amount") }
    if astra_vals != os_vals
      notes << "sorted hits differ"
    end
  end

  # Compare aggregation results
  if has_aggs
    astra_aggs = astra_resp["aggregations"]
    os_aggs = os_resp["aggregations"]
    if astra_aggs.nil? && os_aggs.nil?
      # both nil, fine
    elsif astra_aggs.nil? || os_aggs.nil?
      notes << "aggs: #{ astra_aggs.nil? ? 'astra missing' : 'os missing' }"
    else
      agg_diffs = compare_aggs(astra_aggs, os_aggs)
      notes.concat(agg_diffs) if agg_diffs.any?
    end
  end

  notes
end

# Recursively compare aggregation structures.
# Compares bucket counts and doc_counts; tolerates key ordering differences.
def compare_aggs(astra_aggs, os_aggs)
  diffs = []
  # Find all aggregation names (top-level keys in the aggregation object)
  all_keys = ((astra_aggs.keys rescue []) | (os_aggs.keys rescue [])).sort
  all_keys.each do |key|
    a = astra_aggs[key] rescue nil
    o = os_aggs[key] rescue nil
    if a.nil?
      diffs << "agg '#{key}' missing from astra"
      next
    end
    if o.nil?
      diffs << "agg '#{key}' missing from os"
      next
    end

    # Compare bucket counts
    a_buckets = a["buckets"] if a.is_a?(Hash)
    o_buckets = o["buckets"] if o.is_a?(Hash)
    if a_buckets.is_a?(Array) && o_buckets.is_a?(Array)
      if a_buckets.size != o_buckets.size
        diffs << "agg '#{key}' bucket count: astra=#{a_buckets.size} os=#{o_buckets.size}"
      else
        # Compare doc_counts per bucket
        a_buckets.zip(o_buckets).each_with_index do |(ab, ob), i|
          a_dc = ab["doc_count"] rescue nil
          o_dc = ob["doc_count"] rescue nil
          if a_dc != o_dc
            a_key = ab["key_as_string"] || ab["key"] rescue "?"
            diffs << "agg '#{key}' bucket #{a_key}: doc_count astra=#{a_dc} os=#{o_dc}"
            break # report first mismatch only
          end
        end
      end
    end
  end
  diffs
end

ports = { "astra" => ENV.fetch("ASTRA_QUERY_PORT", "8081").to_i,
          "os" => ENV.fetch("OS_PORT", "9200").to_i }
hosts = { "astra" => ENV.fetch("ASTRA_QUERY_HOST", "localhost"),
          "os" => ENV.fetch("OS_HOST", "localhost") }
os_scheme = ENV.fetch("OS_SCHEME", "http")
os_user = ENV.fetch("OS_USER", "")
os_pw = ENV.fetch("OS_PW", "")
os_tls = ENV.fetch("OS_CURL_INSECURE", "false") == "true" ? "-k" : ""
os_auth = (os_user.empty? && os_pw.empty?) ? "" : ["-u", "#{os_user.empty? ? "admin" : os_user}:#{os_pw}"].shelljoin

curls = { "astra" => "curl -s --fail -H 'Content-Type: application/json' \
  '#{"http://#{hosts["astra"]}:#{ports["astra"]}/_msearch"}' --data-binary ",
          "os" => "curl -s --fail #{os_tls} #{os_auth} '#{os_scheme}://#{hosts["os"]}:#{ports["os"]}/_msearch' \
  -H 'Content-Type: application/json' --data-binary "
}

error_shapes = [
  '"responses":[{"error"',
  'Status 500']

EXPECTED_COMPARISON_ISSUES = {
  autohisto_1000_bucket_agg: "expected: OpenSearch auto_date_histogram 1000-bucket output keeps partially reduced bucket counts; excluded from parity comparisons"
}

EXPECTED_ASTRA_UNSUPPORTED_ISSUES = {
  distance_amount_agg: "expected: KalDB does not register the stats aggregation used by this benchmark query",
  date_histogram_fixed_interval_with_metrics: "expected: KalDB does not register the stats aggregation used by this benchmark query",
  auto_date_histogram_with_metrics: "expected: KalDB does not register the stats aggregation used by this benchmark query"
}

EXPECTED_OS_FAILURE_ISSUES = {
  date_histogram_fixed_interval_with_metrics: "expected: OpenSearch array_index_out_of_bounds_exception for metric date_histogram benchmark",
  auto_date_histogram_with_metrics: "expected: OpenSearch array_index_out_of_bounds_exception for metric auto_date_histogram benchmark"
}

def expected_notes?(notes)
  notes.to_a.any? { |note| note.to_s.start_with?("expected:") }
end

def apply_expected_issues(name, comparison_notes, timings)
  notes = comparison_notes.dup

  if name == :autohisto_1000_bucket_agg
    expected_notes, other_notes = notes.partition do |note|
      note.include?("agg 'dropoffs_over_time' bucket")
    end
    notes = other_notes
    notes << EXPECTED_COMPARISON_ISSUES.fetch(name) if expected_notes.any?
  end

  if EXPECTED_ASTRA_UNSUPPORTED_ISSUES.key?(name)
    unsupported_notes, other_notes = notes.partition do |note|
      note == "aggs: astra missing" || note.start_with?("hit count: astra=0 os=")
    end
    notes = other_notes
    notes << EXPECTED_ASTRA_UNSUPPORTED_ISSUES.fetch(name) if unsupported_notes.any?
  end

  os_failed = timings[1] && timings[1][1] && timings[1][1][0]
  if os_failed && EXPECTED_OS_FAILURE_ISSUES.key?(name)
    notes << EXPECTED_OS_FAILURE_ISSUES.fetch(name)
  end

  notes.uniq
end

subjects = ["astra", "os"]
default_sizes = [
  0,50,100,250,500,1000,2000,3000,4000,5000,6000,7000,8000,9000,10_000,
         # Extra large sizes
         # 100_000, 200_000
]
sizes = ENV.fetch("BENCHMARK_SIZES", default_sizes.join(","))
           .split(",")
           .map { |size| Integer(size.strip) }


# things to pull out of the output
# count of .responses[].hits.hits
# total value of .responses[].hits.total.value
# took, .responses[].took
# responses[].error
#

TO_DIG = {
  hits_all: ["responses", 0, "hits", "hits"],
  hits_total_value: ["responses", 0, "hits", "total", "value"],
  root_took: ["took"],
  resp_took: ["responses", 0, "took"],
  error: ["responses", 0, "error"],
}
def extract_from_output json
  # Extracts the count of hits, total value, took, and error from the output JSON
  # Returns a hash with these values
  TO_DIG.transform_values do |path|
    json.dig(*path)
  end
end

def request_has_aggregations?(request_body)
  response = JSON.parse(request_body.lines.last)
  response.key?("aggs") || response.key?("aggregations")
rescue JSON::ParserError
  request_body.include?('"aggs"') || request_body.include?('"aggregations"')
end


# checks

# jq queries
hits_hits_len = -> (extracted, size) { extracted[:hits_all].size == size }
hits_total_val = -> (extracted, size) { extracted[:hits_total_value] == size}
# bucket_count = -> (extracted, size) { extracted[:buckets].size == size }

# query changes
# increase size
# increase something else
in_2015 = '{"gte":"2015-01-01T00:00:00Z", "lt": "2015-01-02T00:00:00Z"}'
in_2015_all = '{"gte":"2015-01-01T00:00:00Z", "lte": "2016-01-12T00:00:00Z"}'
tz_snippet = '"time_zone": "America/New_York"'
range_date_snippet = %Q!{"range": {"dropoff_datetime": #{in_2015}}}!
range_date_snippet_full = %Q!{"range": {"dropoff_datetime": #{in_2015_all}}}!


queries = {
           autohisto_1000_bucket_agg: [
             %Q!#{range_date_snippet_full},  "aggs": {"dropoffs_over_time": { "auto_date_histogram": {"field": "dropoff_datetime", "buckets": 1000}}}!,
             hits_hits_len],
           autohisto_100_bucket_agg: [
             %Q!#{range_date_snippet_full},  "aggs": {"dropoffs_over_time": { "auto_date_histogram": {"field": "dropoff_datetime", "buckets": 100}}}!,
             hits_hits_len],
           autohisto_5_bucket_agg: [
             %Q!#{range_date_snippet_full},  "aggs": {"dropoffs_over_time": { "auto_date_histogram": {"field": "dropoff_datetime", "buckets": 5}}}!,
             hits_hits_len],
           match_all: ['{"match_all": {}}', hits_hits_len],
           range_smaller: ['{"range": {"total_amount": {"gte": 0.5,"lt": 2}}}', hits_hits_len],
           range: ['{"range": {"total_amount": {"gte": 5,"lt": 15}}}', hits_hits_len],
           # maybe the agg ones should be checked differently?
           distance_amount_agg: [
             %Q!{"bool": {"filter": {"range": {"trip_distance": {"lt": 50,"gte": 0}}}}},"aggs": {"distance_histo": {"histogram": {"field": "trip_distance","interval": 1},"aggs": {"total_amount_stats": {"stats": {"field": "total_amount"}}}}}!,
             hits_hits_len
           ],
           autohisto_agg: [
             %Q!#{range_date_snippet_full},   "aggs": {"dropoffs_over_time": { "auto_date_histogram": {"field": "dropoff_datetime", "buckets": 20}}}!,
             hits_hits_len],
           date_histogram_agg: [
             %Q!#{range_date_snippet_full},   "aggs": {"dropoffs_over_time": { "date_histogram": { "field": "dropoff_datetime", "calendar_interval": "day" } } }!,
             hits_hits_len],
           date_histogram_calendar_interval:[
             %Q!#{range_date_snippet},           "aggs": {"dropoffs_over_time": {"date_histogram": {"field": "dropoff_datetime", "calendar_interval": "month"}}}!,
             hits_hits_len
           ],
           date_histogram_calendar_interval_with_tz: [
             %Q!#{range_date_snippet},           "aggs": {"dropoffs_over_time": {"date_histogram": {"field": "dropoff_datetime", "calendar_interval": "month",#{tz_snippet}}}}!,
             hits_hits_len
           ],
           date_histogram_fixed_interval:[
             %Q!#{range_date_snippet},           "aggs": {"dropoffs_over_time": {"date_histogram": {"field": "dropoff_datetime", "fixed_interval": "60d"}}}!,
             hits_hits_len
           ],
           date_histogram_fixed_interval_with_tz: [
             %Q!#{range_date_snippet},           "aggs": {"dropoffs_over_time": {"date_histogram": {"field": "dropoff_datetime", "fixed_interval": "60d",#{tz_snippet}}}}!,
             hits_hits_len
           ],
           date_histogram_fixed_interval_with_metrics: [
             %Q!#{range_date_snippet},           "aggs": {"dropoffs_over_time": {"date_histogram": {"field": "dropoff_datetime","fixed_interval": "60d"},"aggs": {"total_amount": {"stats": { "field": "total_amount" }},"tip_amount": {"stats": { "field": "tip_amount" }},"trip_distance": {"stats": { "field": "trip_distance"}}}}}!,
             hits_hits_len
           ],
           auto_date_histogram: [
             %Q!#{range_date_snippet},           "aggs": {"dropoffs_over_time": {"auto_date_histogram": {"field": "dropoff_datetime", "buckets": "12"}}}!,
             hits_hits_len
           ],
           auto_date_histogram_with_tz: [
             %Q!#{range_date_snippet},           "aggs": {"dropoffs_over_time": {"auto_date_histogram": {"field": "dropoff_datetime","buckets": "13",#{tz_snippet}}}}!,
             hits_hits_len
           ],
           auto_date_histogram_with_metrics: [
             %Q!#{range_date_snippet},           "aggs": {"dropoffs_over_time": {"auto_date_histogram": {"field": "dropoff_datetime","buckets": "12"},"aggs": {"total_amount": { "stats": { "field": "total_amount" } },"tip_amount": { "stats": { "field": "tip_amount" } },"trip_distance": { "stats": { "field": "trip_distance" } }}}}!,
             hits_hits_len
           ],
           sorting_desc: [
             %Q!{"match_all": {}},"sort" : [{"tip_amount" : "desc"},{"total_amount" : "desc"}]!,
             hits_hits_len
           ],
           sorting_asc: [
             %Q!{"match_all": {}},"sort" : [{"tip_amount" : "asc"},{"total_amount" : "asc"}]!,
             hits_hits_len
           ],
}

if ENV["BENCHMARK_QUERIES"] && !ENV["BENCHMARK_QUERIES"].empty?
  selected_queries = ENV["BENCHMARK_QUERIES"].split(",").map { |name| name.strip.to_sym }
  missing_queries = selected_queries - queries.keys
  abort("Unknown BENCHMARK_QUERIES: #{missing_queries.join(", ")}") unless missing_queries.empty?
  queries = queries.slice(*selected_queries)
end

stats = []
stats << [:name, :count, :astra_ms, :astra_asterisk, :astra_asterisk_why, :os_ms, :os_asterisk, :os_asterisk_why, :comparison_notes]

def color_code ratio, mismatch: false, expected: false
  if expected
    return "\033[97;100mx#{ratio.round(2).to_s.ljust(4)}\033[0m"
  end
  if mismatch
    # gray background with "!" to indicate results didn't match — ratio is unreliable
    return "\033[97;90m!#{ratio.round(2).to_s.ljust(4)}\033[0m"
  end
  # red 41
  # green 42
  num = if ratio == 1
          40
        elsif ratio > 1.5
          101
        elsif ratio > 1
          41
        elsif ratio < 0.5
          102
        elsif ratio < 1
          42
        else
          105
        end
  "\033[97;#{num}m #{ratio.round(2).to_s.ljust(4)}\033[0m"
end

at_exit do
  puts "\n\n\n"
  puts "="*80
  # name, count, astra_ms, astrisk, astrisk_why, os_ms, os_asterisk, os_asterisk_why
  stats_group_by = stats[1..-1].group_by(&:first)
  puts "#{" "*56}size #{stats_group_by.values.find{|v|v}.map{|r|r[1]}.uniq.map(&:to_s).map{|i|i.sub(/000$/,'k').rjust 5}.join()}"

  stats_group_by.each do |name, rows|
    rows = rows.group_by{|r|r[1]}.map do |count, rows|
      r = rows[0]
      notes = rows.flat_map { |x| x[8].to_a }
      expected = expected_notes?(notes)
      mismatch = notes.any? && !expected
      [r[0], r[1], rows.map{|x|x[2]}.sum.to_f/rows.size, r[3], r[4], rows.map{|x|x[5]}.sum.to_f/rows.size, r[6], r[7], mismatch, expected]
    end.sort_by{|r|r[1]}
    puts "#{name.to_s.ljust 60} #{rows.map{|r|color_code(r[2].to_f / r[5], mismatch: r[8], expected: r[9])}.join ''}"
  end
  puts
  puts "Legend: ratio = astra_ms/os_ms. >1 = OS faster (red), <1 = Astra faster (green), ! = unexpected result mismatch, x = expected xfail/excluded row"
end

current_time = Time.now
run_id = ENV.fetch("BENCHMARK_RUN_ID", current_time.strftime("%Y-%m-%d-%H-%M-%S"))
output_root = ENV.fetch("BENCHMARK_OUTPUT_ROOT", "output")
results_root = ENV.fetch("BENCHMARK_RESULTS_ROOT", "results")
results_prefix = ENV.fetch("BENCHMARK_RESULTS_PREFIX", "benchmark")
output_dir = File.join(output_root, run_id)
results_file = ENV.fetch("BENCHMARK_RESULTS_FILE", File.join(results_root, "#{results_prefix}.#{run_id}.csv"))

Dir.mkdir(output_root) unless Dir.exist?(output_root)
Dir.mkdir(output_dir) unless Dir.exist?(output_dir)

iterations = (ARGV.first || 1).to_i
iterations = 1 if iterations <= 0
iterations.times do |iteration|
  puts
  puts
  puts "Iteration #{iteration+1}/#{iterations} at #{current_time.strftime "%Y-%m-%d %H:%M:%S"}"
  puts "="*80
  puts "="*80
  queries.to_a.each.with_index do |(name, (query,jq_query)), i|
    puts
    puts "="*80
    puts "#{((i+1).to_s+"/#{queries.size}").ljust(7)} #{name.to_s.tr('_', ' ').capitalize} - #{query}"
    puts "="*80

    sizes.each do |count|
      timings = subjects.map do |subject|
        raw_out = nil
        failed = false
        fail_message = []
        request_body = "{\"index\":\"test\"}\n{\"query\": #{query}, \"size\": #{count}}\n"
        timing = Benchmark.measure("#{subject} #{count}".ljust(60)) do
          raw_out = `#{curls[subject]} '#{request_body}'`
        end
        File.write(File.join(output_dir, "#{name}-#{count}-#{subject}.json"), raw_out)
        out = raw_out
        json_out = begin
                     JSON.parse(out)
                   rescue JSON::ParserError => e
                     print 'E'
                     failed = true
                     fail_message << "parse error: #{e.message}"
                     {}
                   end
        extracted_vals = extract_from_output(json_out)
        # per call validations:
        # - exit status
        # - matches an error shape
        # - error in response
        if $?.exitstatus != 0
          rerun_out = `#{curls[subject].sub"-s","-vvv"} '#{request_body}'`
          rerun_out = "rerun result:\n> #{rerun_out.gsub("\n", "> ")}"
          failed = true
          fail_message << "Error with #{subject} #{$?.exitstatus}\n#{rerun_out}"
        end
        matching_error = error_shapes.find{|x|out.include?(x)}
        if matching_error
          failed = true
          fail_message << "found error shape like #{matching_error}"
        end
        if extracted_vals[:error]
          failed = true
          fail_message << "had error: #{extracted_vals[:error]}"
        end
        if json_out.dig("responses", 0).nil?
          failed = true
          fail_message << "missing msearch response"
        end
        if extracted_vals[:hits_total_value].nil?
          failed = true
          fail_message << "missing hits.total.value"
        elsif extracted_vals[:hits_total_value].to_i <= 0
          failed = true
          fail_message << "hits.total.value is #{extracted_vals[:hits_total_value]}"
        end
        if request_has_aggregations?(request_body)
          aggregations = json_out.dig("responses", 0, "aggregations")
          unless aggregations.is_a?(Hash) && !aggregations.empty?
            failed = true
            fail_message << "missing aggregations"
          end
        elsif count.positive? && (extracted_vals[:hits_all].nil? || extracted_vals[:hits_all].empty?)
          failed = true
          fail_message << "no returned hits"
        end
        if extracted_vals[:hits_all] && extracted_vals[:hits_all].size > count
          failed = true
          fail_message << "hit count exceeded requested size [#{extracted_vals[:hits_all].size}] > #{count}"
        end
        if extracted_vals[:hits_all] && extracted_vals[:hits_total_value]
          expected_hits = [count, extracted_vals[:hits_total_value].to_i].min
          if extracted_vals[:hits_all].size != expected_hits
            failed = true
            fail_message << "hit count didn't match capped total [#{extracted_vals[:hits_all].size}] != #{expected_hits}"
          end
        end
        [timing, [failed, fail_message.join(', ')], raw_out]
      end
      # comparing results between astra and os:
      parsed_responses = timings.map(&:last).map { |json|
        JSON.parse(json) rescue nil
      }
      comparison_notes = compare_responses(parsed_responses[0], parsed_responses[1], query, count)
      comparison_notes = apply_expected_issues(name, comparison_notes, timings)
      stats << [name, *stat_output(count, timings, ->(c,o) {o}), comparison_notes]
      print output_line count, timings, ->(c,o) { o }
      if comparison_notes.empty?
        puts
      else
        puts " #{comparison_notes.join('; ')}"
      end
    end
  end
end

Dir.mkdir(results_root) unless Dir.exist?(results_root)
CSV.open(results_file, "w") do |csv|
  stats.each do |row|
    csv << row
  end
end
