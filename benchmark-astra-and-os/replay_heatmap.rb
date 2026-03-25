#!/usr/bin/env ruby
# Replay a heatmap from saved benchmark output files and CSV timing data.
# Usage: ruby replay_heatmap.rb [output_dir] [csv_file]
#   Defaults to most recent output dir and CSV in results/.

require 'json'
require 'csv'

def compare_responses(astra_json, os_json, query_str, requested_size)
  notes = []
  return ["comparison skipped (parse error)"] if astra_json.nil? || os_json.nil?
  astra_resp = astra_json.dig("responses", 0)
  os_resp = os_json.dig("responses", 0)
  return ["comparison skipped (missing response)"] unless astra_resp && os_resp

  has_aggs = query_str.include?('"aggs"')
  has_sort = query_str.include?('"sort"')

  astra_hit_ct = (astra_resp.dig("hits", "hits") || []).size
  os_hit_ct = (os_resp.dig("hits", "hits") || []).size
  notes << "hit count: astra=#{astra_hit_ct} os=#{os_hit_ct}" if astra_hit_ct != os_hit_ct

  if has_sort && requested_size > 0
    astra_vals = (astra_resp.dig("hits", "hits") || []).map { |h| h.dig("_source", "total_amount") }
    os_vals = (os_resp.dig("hits", "hits") || []).map { |h| h.dig("_source", "total_amount") }
    notes << "sorted hits differ" if astra_vals != os_vals
  end

  if has_aggs
    astra_aggs = astra_resp["aggregations"]
    os_aggs = os_resp["aggregations"]
    if astra_aggs.nil? && os_aggs.nil?
      # both nil
    elsif astra_aggs.nil? || os_aggs.nil?
      notes << "aggs: #{astra_aggs.nil? ? 'astra missing' : 'os missing'}"
    else
      notes.concat(compare_aggs(astra_aggs, os_aggs))
    end
  end
  notes
end

def compare_aggs(astra_aggs, os_aggs)
  diffs = []
  all_keys = ((astra_aggs.keys rescue []) | (os_aggs.keys rescue [])).sort
  all_keys.each do |key|
    a = astra_aggs[key] rescue nil
    o = os_aggs[key] rescue nil
    next diffs << "agg '#{key}' missing from astra" if a.nil?
    next diffs << "agg '#{key}' missing from os" if o.nil?
    a_buckets = a["buckets"] if a.is_a?(Hash)
    o_buckets = o["buckets"] if o.is_a?(Hash)
    if a_buckets.is_a?(Array) && o_buckets.is_a?(Array)
      if a_buckets.size != o_buckets.size
        diffs << "agg '#{key}' bucket count: astra=#{a_buckets.size} os=#{o_buckets.size}"
      else
        a_buckets.zip(o_buckets).each do |(ab, ob)|
          a_dc = ab["doc_count"] rescue nil
          o_dc = ob["doc_count"] rescue nil
          if a_dc != o_dc
            a_key = ab["key_as_string"] || ab["key"] rescue "?"
            diffs << "agg '#{key}' bucket #{a_key}: doc_count differs"
            break
          end
        end
      end
    end
  end
  diffs
end

def color_code(ratio, mismatch: false)
  if mismatch
    return "\033[97;90m!#{ratio.round(2).to_s.ljust(4)}\033[0m"
  end
  num = if ratio == 1 then 40
        elsif ratio > 1.5 then 101
        elsif ratio > 1 then 41
        elsif ratio < 0.5 then 102
        elsif ratio < 1 then 42
        else 105
        end
  "\033[97;#{num}m #{ratio.round(2).to_s.ljust(4)}\033[0m"
end

# Resolve output dir and CSV file
output_dir = ARGV[0] || Dir.glob("output/*").sort.last
csv_file = ARGV[1] || Dir.glob("results/*.csv").sort.last

abort "No output directory found" unless output_dir && Dir.exist?(output_dir)
abort "No CSV file found" unless csv_file && File.exist?(csv_file)

puts "Replaying: #{output_dir} + #{csv_file}"
puts

# Detect which queries have aggs or sort from file names + content
agg_queries = {}
sort_queries = {}
Dir.glob("#{output_dir}/*-0-astra.json").each do |f|
  name = File.basename(f).sub(/-0-astra\.json$/, '')
  json = JSON.parse(File.read(f)) rescue next
  resp = json.dig("responses", 0) || next
  agg_queries[name] = true if resp["aggregations"]
end
Dir.glob("#{output_dir}/*-50-astra.json").each do |f|
  name = File.basename(f).sub(/-50-astra\.json$/, '')
  astra = JSON.parse(File.read(f)) rescue next
  hits = astra.dig("responses", 0, "hits", "hits") || next
  # If all hits have sort values that look like timestamps (>1e12), and the
  # corresponding OS file sorts differently, it's a sort query
  os_file = f.sub("-astra.json", "-os.json")
  next unless File.exist?(os_file)
  os = JSON.parse(File.read(os_file)) rescue next
  os_hits = os.dig("responses", 0, "hits", "hits") || next
  astra_sorts = hits.map { |h| h["sort"] }.compact
  os_sorts = os_hits.map { |h| h["sort"] }.compact
  if astra_sorts.any? && os_sorts.any? && astra_sorts != os_sorts
    sort_queries[name] = true
  end
end

sizes = [0, 50, 100, 250, 500, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]

# Read CSV for timing data (average across iterations)
timing_sums = Hash.new { |h, k| h[k] = { astra: 0.0, os: 0.0, count: 0 } }
CSV.foreach(csv_file, headers: true) do |row|
  key = [row["name"], row["count"].to_i]
  timing_sums[key][:astra] += row["astra_ms"].to_f
  timing_sums[key][:os] += row["os_ms"].to_f
  timing_sums[key][:count] += 1
end
timings = {}
timing_sums.each do |key, v|
  timings[key] = { astra: v[:astra] / v[:count], os: v[:os] / v[:count] }
end

# Collect query names preserving CSV order
query_names = []
seen = {}
CSV.foreach(csv_file, headers: true) do |row|
  name = row["name"]
  unless seen[name]
    query_names << name
    seen[name] = true
  end
end

# Build heatmap
puts "=" * 80
puts "#{" " * 56}size #{sizes.map { |s| s.to_s.sub(/000$/, 'k').rjust(5) }.join}"
puts

query_names.each do |name|
  cells = sizes.map do |size|
    t = timings[[name, size]]
    next "     " unless t

    astra_file = "#{output_dir}/#{name}-#{size}-astra.json"
    os_file = "#{output_dir}/#{name}-#{size}-os.json"
    next "     " unless File.exist?(astra_file) && File.exist?(os_file)

    astra_json = JSON.parse(File.read(astra_file)) rescue nil
    os_json = JSON.parse(File.read(os_file)) rescue nil

    query_str = ""
    query_str += '"aggs"' if agg_queries[name]
    query_str += '"sort"' if sort_queries[name]

    notes = compare_responses(astra_json, os_json, query_str, size)
    ratio = t[:os] > 0 ? t[:astra] / t[:os] : 0
    color_code(ratio, mismatch: notes.any?)
  end
  puts "#{name.ljust(56)} #{cells.join}"
end

puts
puts "Legend: ratio = astra_ms/os_ms. >1 = OS faster (red), <1 = Astra faster (green), ! = results differ (ratio unreliable)"
