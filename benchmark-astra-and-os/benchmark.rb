#!/usr/bin/env ruby
require 'benchmark'
require 'csv'
require 'json'

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

ports = { "astra" => ENV.fetch("ASTRA_QUERY_PORT", "8080").to_i,
          "os" => ENV.fetch("OS_PORT", "9200").to_i }
os_scheme = ENV.fetch("OS_SCHEME", "https")
os_auth = os_scheme == "https" ? "-ku admin:$OS_PW" : ""

curls = { "astra" => "curl -s --fail -H 'Content-Type: application/json' \
  'http://localhost:#{ports["astra"]}/_msearch' --data-binary ",
          "os" => "curl -s --fail #{os_auth} '#{os_scheme}://localhost:#{ports["os"]}/_msearch' \
  -H 'Content-Type: application/json' --data-binary "
}

error_shapes = [
  '"responses":[{"error"',
  'Status 500']

subjects = ["astra", "os"]
sizes = [
  0,50,100,250,500,1000,2000,3000,4000,5000,6000,7000,8000,9000,10_000,
         # Extra large sizes
         # 100_000, 200_000
]


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


# checks

# jq queries
hits_hits_len = -> (extracted, size) { extracted[:hits_all].size == size }
hits_total_val = -> (extracted, size) { extracted[:hits_total_value] == size}
# bucket_count = -> (extracted, size) { extracted[:buckets].size == size }

# query changes
# increase size
# increase something else
in_2015 =   '{"gte":"2015-01-01 00:00:00", "lt": "2015-01-02 00:00:00", "format": "yyyy-MM-dd HH:mm:ss"}'
in_2015_slashes = '{"gte": "01/01/2015", "lte": "21/01/2015", "format": "dd/MM/yyyy"}'
in_2015_slashes = '{"gte": "01/01/2015", "lte": "12/01/2016", "format": "dd/MM/yyyy"}'
tz_snippet = '"time_zone": "America/New_York"'
range_date_snippet = %Q!{"range": {"dropoff_datetime": #{in_2015}}}!
range_date_snippet_slashes = %Q!{"range": {"dropoff_datetime": #{in_2015_slashes}}}!
range_date_snippet_invalid = %Q!{"range": {"dropoff_datetime": #{in_2015_slashes}}}!


queries = {
           autohisto_1000_bucket_agg: [
             %Q!#{range_date_snippet_invalid},  "aggs": {"dropoffs_over_time": { "auto_date_histogram": {"field": "dropoff_datetime", "buckets": 1000}}}!,
             hits_hits_len],
           autohisto_100_bucket_agg: [
             %Q!#{range_date_snippet_invalid},  "aggs": {"dropoffs_over_time": { "auto_date_histogram": {"field": "dropoff_datetime", "buckets": 100}}}!,
             hits_hits_len],
           autohisto_5_bucket_agg: [
             %Q!#{range_date_snippet_invalid},  "aggs": {"dropoffs_over_time": { "auto_date_histogram": {"field": "dropoff_datetime", "buckets": 5}}}!,
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
             %Q!#{range_date_snippet_slashes},   "aggs": {"dropoffs_over_time": { "auto_date_histogram": {"field": "dropoff_datetime", "buckets": 20}}}!,
             hits_hits_len],
           date_histogram_agg: [
             %Q!#{range_date_snippet_slashes},   "aggs": {"dropoffs_over_time": { "date_histogram": { "field": "dropoff_datetime", "calendar_interval": "day" } } }!,
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
             %Q!{"match_all": {}},"sort" : [{"tip_amount" : "desc"}]!,
             hits_hits_len
           ],
           sorting_asc: [
             %Q!{"match_all": {}},"sort" : [{"tip_amount" : "asc"}]!,
             hits_hits_len
           ],
}

stats = []
stats << [:name, :count, :astra_ms, :astra_asterisk, :astra_asterisk_why, :os_ms, :os_asterisk, :os_asterisk_why]

def color_code ratio
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
    rows = rows.group_by{|r|r[1]}.map {|count, rows| r = rows[0]; [r[0], r[1], rows.map{|x|x[2]}.sum.to_f/rows.size, r[3], r[4], rows.map{|x|x[5]}.sum.to_f/rows.size, r[6], r[7]]}.sort_by{|r|r[1]}
    puts "#{name.to_s.ljust 60} #{rows.map{|r|color_code(r[2].to_f / r[5])}.join ''}"
  end
end

current_time = Time.now
Dir.mkdir("output") unless Dir.exist?("output")
Dir.mkdir("output/#{current_time.strftime "%Y-%m-%d-%H-%M-%S"}") unless Dir.exist?("output/#{current_time.strftime "%Y-%m-%d-%H-%M-%S"}")

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
        File.write("output/#{current_time.strftime "%Y-%m-%d-%H-%M-%S"}/#{name}-#{count}-#{subject}.json", raw_out)
        out = raw_out
        json_out = begin
                     JSON.parse(out)
                   rescue
                     print 'E'
                     {}
                     end
        extracted_vals = extract_from_output(json_out)
        # per call validations:
        # - exit status
        # - matches an error shape
        # - error in response
        # - TODO more specific checks
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
        if extracted_vals[:hits_all].nil? || extracted_vals[:hits_all].empty?
          # no hits
        end
        if extracted_vals[:hits_all] && extracted_vals[:hits_all].size != count
          failed = true
          fail_message << "hit count didn't match [#{extracted_vals[:hits_all].size}] != #{count}"
        end
        if extracted_vals[:hits_total_val].to_i == 0
          # and expected there to be a total?
          # failed = true
          # fail_message << "total: 0 #{extracted_vals[:hits_total_val].inspect}"
        end

        [timing, [failed, fail_message.join(', ')], raw_out]
      end
      # comparing results between astra and os:
      parsed_responses = timings.map(&:last).map { |json|
        JSON.parse(json) rescue nil
      }
      stats << [name, *stat_output(count, timings, ->(c,o) {o})]
      print output_line count, timings, ->(c,o) { o }
      comparison_notes = compare_responses(parsed_responses[0], parsed_responses[1], query, count)
      if comparison_notes.empty?
        puts
      else
        puts " #{comparison_notes.join('; ')}"
      end
    end
  end
end

Dir.mkdir("results") unless Dir.exist?("results")
CSV.open("results/benchmark.#{Time.now.strftime "%Y-%m-%d-%H-%M-%S"}.csv", "w") do |csv|
  stats.each do |row|
    csv << row
  end
end
