#!/usr/bin/env ruby

require "csv"
require "json"
require "optparse"

def resolve_run(path)
  expanded = File.expand_path(path)
  if File.directory?(expanded)
    csv_path = File.join(expanded, "benchmark.csv")
    run_path = File.join(expanded, "run.json")
    metadata = File.exist?(run_path) ? JSON.parse(File.read(run_path)) : {}
    [csv_path, metadata]
  else
    [expanded, {}]
  end
end

def parse_notes(field)
  JSON.parse(field.to_s)
rescue JSON::ParserError
  [field.to_s]
end

def load_rows(csv_path)
  CSV.read(csv_path, headers: true).map do |row|
    {
      key: [row["name"], row["count"].to_i],
      name: row["name"],
      count: row["count"].to_i,
      astra_ms: row["astra_ms"].to_f,
      os_ms: row["os_ms"].to_f,
      notes: parse_notes(row["comparison_notes"])
    }
  end
end

options = {
  count: nil,
  limit: 10
}

OptionParser.new do |parser|
  parser.banner = "Usage: ruby compare_runs.rb [options] OLD_RUN NEW_RUN"
  parser.on("--count N", Integer, "Only compare one size value") { |value| options[:count] = value }
  parser.on("--limit N", Integer, "Rows per section (default: 10)") { |value| options[:limit] = value }
end.parse!

abort("Need OLD_RUN and NEW_RUN") unless ARGV.size == 2

old_csv, old_meta = resolve_run(ARGV[0])
new_csv, new_meta = resolve_run(ARGV[1])
abort("Missing CSV: #{old_csv}") unless File.exist?(old_csv)
abort("Missing CSV: #{new_csv}") unless File.exist?(new_csv)

old_rows = load_rows(old_csv)
new_rows = load_rows(new_csv)

old_map = old_rows.to_h { |row| [row[:key], row] }
new_map = new_rows.to_h { |row| [row[:key], row] }
shared_keys = old_map.keys & new_map.keys
shared_keys.select! { |(_, count)| count == options[:count] } if options[:count]

comparisons = shared_keys.map do |key|
  old_row = old_map[key]
  new_row = new_map[key]
  next if !old_row[:notes].empty? || !new_row[:notes].empty?
  {
    name: old_row[:name],
    count: old_row[:count],
    old_astra_ms: old_row[:astra_ms],
    new_astra_ms: new_row[:astra_ms],
    old_os_ms: old_row[:os_ms],
    new_os_ms: new_row[:os_ms],
    astra_delta_ms: new_row[:astra_ms] - old_row[:astra_ms],
    os_delta_ms: new_row[:os_ms] - old_row[:os_ms],
    old_ratio: old_row[:astra_ms] / old_row[:os_ms],
    new_ratio: new_row[:astra_ms] / new_row[:os_ms],
    ratio_delta: (new_row[:astra_ms] / new_row[:os_ms]) - (old_row[:astra_ms] / old_row[:os_ms])
  }
end.compact

def print_section(title, rows)
  puts
  puts title
  puts "-" * title.length
  if rows.empty?
    puts "(none)"
    return
  end

  rows.each do |row|
    puts "#{row[:name]} size=#{row[:count]} | astra #{row[:old_astra_ms]} -> #{row[:new_astra_ms]} ms | os #{row[:old_os_ms]} -> #{row[:new_os_ms]} ms | ratio #{format("%.2f", row[:old_ratio])} -> #{format("%.2f", row[:new_ratio])}"
  end
end

old_label = old_meta["label"] || File.basename(File.dirname(old_csv))
new_label = new_meta["label"] || File.basename(File.dirname(new_csv))

puts "Old: #{old_label} (#{old_csv})"
puts "New: #{new_label} (#{new_csv})"
puts "Comparable rows: #{comparisons.size}"
puts "Filter count: #{options[:count].nil? ? 'all' : options[:count]}"

print_section(
  "Biggest Astra Improvements",
  comparisons.sort_by { |row| row[:astra_delta_ms] }.first(options[:limit])
)

print_section(
  "Biggest Astra Regressions",
  comparisons.sort_by { |row| -row[:astra_delta_ms] }.first(options[:limit])
)

print_section(
  "Biggest Ratio Improvements",
  comparisons.sort_by { |row| row[:ratio_delta] }.first(options[:limit])
)

print_section(
  "Biggest Ratio Regressions",
  comparisons.sort_by { |row| -row[:ratio_delta] }.first(options[:limit])
)
