#!/usr/bin/env ruby

require "csv"
require "fileutils"
require "json"
require "open3"
require "optparse"
require "rbconfig"
require "socket"
require "time"

require_relative "jvm_capture"

SCRIPT_DIR = File.expand_path(__dir__)
REPO_ROOT = File.expand_path("..", SCRIPT_DIR)
CLICKBENCH_QUERY_IDS = (0..42).to_a.freeze
BENCHMARK_ENV_NAMES = %w[
  ASTRA_INDEX_NAME
  ASTRA_BULK_CURL_MAX_TIME
  ASTRA_DATASET_THROUGHPUT_BYTES
  ASTRA_DEPLOYMENT
  ASTRA_PARTITION_MAX_CAPACITY_BYTES
  BENCHMARK_QUERIES
  BULK_GROUP_SIZE
  CLICKBENCH_TRACK_TOTAL_HITS
  CLICKBENCH_QUERY_PROFILE
  CB_PATCH_Q19_USER_ID
  CB_Q19_USER_ID
  DATASET
  INDEX_NAME
  INDEXER_MAX_BYTES_PER_CHUNK
  INDEXER_MAX_MESSAGES_PER_CHUNK
  MAX_RECORDS
  OPENSEARCH_JAVA_OPTS
  OS_NUMBER_OF_SHARDS
  OS_REQUEST_CACHE
  SPLITS_COUNT
  SPLIT_SIZE
].freeze

def run_cmd(*cmd, chdir: REPO_ROOT)
  stdout, stderr, status = Open3.capture3(*cmd, chdir: chdir)
  {
    "cmd" => cmd,
    "stdout" => stdout,
    "stderr" => stderr,
    "success" => status.success?,
    "exit_status" => status.exitstatus
  }
end

def slugify(text)
  slug = text.to_s.downcase.gsub(/[^a-z0-9]+/, "-").gsub(/\A-+|-+\z/, "")
  slug.empty? ? "run" : slug
end

def read_note(options)
  note = options[:note].to_s.dup
  if options[:note_file]
    file_note = File.read(options[:note_file]).strip
    note = [note, file_note].reject(&:empty?).join("\n")
  end
  note
end

def parse_versions
  pom = File.read(File.join(REPO_ROOT, "astra", "pom.xml"))
  {
    "lucene_version" => pom[/<lucene\.version>([^<]+)<\/lucene\.version>/, 1],
    "opensearch_version" => pom[/<opensearch\.version>([^<]+)<\/opensearch\.version>/, 1]
  }
end

def git_metadata
  head_sha = run_cmd("git", "rev-parse", "HEAD")
  branch = run_cmd("git", "branch", "--show-current")
  status = run_cmd("git", "status", "--short")
  subject = run_cmd("git", "show", "-s", "--format=%s", "HEAD")
  {
    "repo_root" => REPO_ROOT,
    "head_sha" => head_sha["stdout"].strip,
    "branch" => branch["stdout"].strip,
    "head_subject" => subject["stdout"].strip,
    "dirty" => !status["stdout"].strip.empty?,
    "status_short" => status["stdout"],
    "diff" => run_cmd("git", "diff", "--binary", "--no-ext-diff")["stdout"],
    "diff_cached" => run_cmd("git", "diff", "--cached", "--binary", "--no-ext-diff")["stdout"]
  }
end

def parse_notes_field(field)
  JSON.parse(field.to_s)
rescue JSON::ParserError
  [field.to_s]
end

def benchmark_environment
  BENCHMARK_ENV_NAMES.each_with_object({}) do |name, env|
    env[name] = ENV[name] if ENV.key?(name)
  end
end

def expected_notes?(notes)
  notes.to_a.any? { |note| note.to_s.start_with?("expected:") }
end

def summarize_csv(csv_path)
  rows = CSV.read(csv_path, headers: true)
  comparable_rows = []
  mismatch_rows = []
  expected_rows = []
  astra_failed_rows = 0
  os_failed_rows = 0
  expected_astra_failed_rows = 0
  expected_os_failed_rows = 0

  rows.each do |row|
    notes = parse_notes_field(row["comparison_notes"])
    expected = expected_notes?(notes)
    astra_failed = row["astra_asterisk"] == "true"
    os_failed = row["os_asterisk"] == "true"
    if expected
      expected_astra_failed_rows += 1 if astra_failed
      expected_os_failed_rows += 1 if os_failed
    else
      astra_failed_rows += 1 if astra_failed
      os_failed_rows += 1 if os_failed
    end
    entry = {
      "name" => row["name"],
      "count" => row["count"].to_i,
      "astra_ms" => row["astra_ms"].to_f,
      "os_ms" => row["os_ms"].to_f,
      "notes" => notes
    }
    if expected
      expected_rows << entry
    elsif notes.empty?
      comparable_rows << entry
    else
      mismatch_rows << entry
    end
  end

  astra_faster = comparable_rows.count { |row| row["astra_ms"] < row["os_ms"] }
  os_faster = comparable_rows.count { |row| row["astra_ms"] > row["os_ms"] }
  ties = comparable_rows.count { |row| row["astra_ms"] == row["os_ms"] }

  ratio_rows = comparable_rows.select { |row| row["os_ms"].positive? }.map do |row|
    row.merge("ratio" => row["astra_ms"] / row["os_ms"])
  end

  {
    "total_rows" => rows.size,
    "comparable_rows" => comparable_rows.size,
    "mismatch_rows" => mismatch_rows.size,
    "expected_rows" => expected_rows.size,
    "astra_failed_rows" => astra_failed_rows,
    "os_failed_rows" => os_failed_rows,
    "expected_astra_failed_rows" => expected_astra_failed_rows,
    "expected_os_failed_rows" => expected_os_failed_rows,
    "failed_rows" => [astra_failed_rows, os_failed_rows].max,
    "astra_faster_rows" => astra_faster,
    "os_faster_rows" => os_faster,
    "tie_rows" => ties,
    "worst_ratios" => ratio_rows.sort_by { |row| -row["ratio"] }.first(5),
    "best_ratios" => ratio_rows.sort_by { |row| row["ratio"] }.first(5)
  }
end

def write_comparison_csv(path, benchmark_csv_path)
  rows = CSV.read(benchmark_csv_path, headers: true)
  CSV.open(path, "w") do |csv|
    csv << [
      "query",
      "size",
      "kaldb_median_ms",
      "kaldb_trimmed_avg_ms",
      "kaldb_min_ms",
      "kaldb_max_ms",
      "opensearch_median_ms",
      "opensearch_trimmed_avg_ms",
      "opensearch_min_ms",
      "opensearch_max_ms",
      "delta_median_ms",
      "kaldb_opensearch_median_ratio"
    ]
    rows.each do |row|
      kaldb_ms = row["astra_ms"].to_f
      opensearch_ms = row["os_ms"].to_f
      ratio = opensearch_ms.positive? ? kaldb_ms / opensearch_ms : 0
      csv << [
        row["name"],
        row["count"],
        format("%.3f", kaldb_ms),
        format("%.3f", row["astra_trimmed_avg_ms"] || kaldb_ms),
        format("%.3f", row["astra_min_ms"] || kaldb_ms),
        format("%.3f", row["astra_max_ms"] || kaldb_ms),
        format("%.3f", opensearch_ms),
        format("%.3f", row["os_trimmed_avg_ms"] || opensearch_ms),
        format("%.3f", row["os_min_ms"] || opensearch_ms),
        format("%.3f", row["os_max_ms"] || opensearch_ms),
        format("%+.3f", kaldb_ms - opensearch_ms),
        format("%.3f", ratio)
      ]
    end
  end
end

def seconds_or_nil(ms)
  return nil if ms.nil? || ms.to_s.empty?

  (Float(ms) / 1000.0).round(6)
end

def row_failed?(row, prefix)
  row["#{prefix}_asterisk"] == "true"
end

def samples_by_query(samples_csv_path)
  samples = Hash.new do |hash, query|
    hash[query] = {
      "kaldb_seconds" => [],
      "opensearch_seconds" => []
    }
  end
  return samples unless File.exist?(samples_csv_path)

  CSV.read(samples_csv_path, headers: true).each do |row|
    query = row["name"]
    samples[query]["kaldb_seconds"] <<
      (row_failed?(row, "astra") ? nil : seconds_or_nil(row["astra_ms"]))
    samples[query]["opensearch_seconds"] <<
      (row_failed?(row, "os") ? nil : seconds_or_nil(row["os_ms"]))
  end
  samples
end

def benchmark_rows_by_query(benchmark_csv_path)
  return {} unless File.exist?(benchmark_csv_path)

  CSV.read(benchmark_csv_path, headers: true).each_with_object({}) do |row, rows|
    rows[row["name"]] = row
  end
end

def fixed_clickbench_slots(samples_csv_path, benchmark_csv_path)
  samples = samples_by_query(samples_csv_path)
  benchmark_rows = benchmark_rows_by_query(benchmark_csv_path)

  CLICKBENCH_QUERY_IDS.map do |query_id|
    query = "q#{query_id}"
    row = benchmark_rows[query]
    {
      "query" => query,
      "kaldb_seconds" => samples.dig(query, "kaldb_seconds"),
      "opensearch_seconds" => samples.dig(query, "opensearch_seconds"),
      "kaldb_median_ms" => row && Float(row["astra_ms"]),
      "opensearch_median_ms" => row && Float(row["os_ms"]),
      "kaldb_trimmed_avg_ms" => row && Float(row["astra_trimmed_avg_ms"]),
      "opensearch_trimmed_avg_ms" => row && Float(row["os_trimmed_avg_ms"])
    }
  end
end

def write_clickbench_line_file(path, slots, system_key)
  File.open(path, "w") do |file|
    slots.each do |slot|
      values = slot[system_key]
      file.puts("#{JSON.generate(values.nil? || values.empty? ? nil : values)},")
    end
  end
end

def write_clickbench_slot_csv(path, slots)
  CSV.open(path, "w") do |csv|
    csv << [
      "query",
      "kaldb_seconds",
      "opensearch_seconds",
      "kaldb_median_ms",
      "opensearch_median_ms",
      "kaldb_trimmed_avg_ms",
      "opensearch_trimmed_avg_ms"
    ]
    slots.each do |slot|
      csv << [
        slot["query"],
        JSON.generate(slot["kaldb_seconds"].nil? || slot["kaldb_seconds"].empty? ? nil : slot["kaldb_seconds"]),
        JSON.generate(slot["opensearch_seconds"].nil? || slot["opensearch_seconds"].empty? ? nil : slot["opensearch_seconds"]),
        slot["kaldb_median_ms"] && format("%.3f", slot["kaldb_median_ms"]),
        slot["opensearch_median_ms"] && format("%.3f", slot["opensearch_median_ms"]),
        slot["kaldb_trimmed_avg_ms"] && format("%.3f", slot["kaldb_trimmed_avg_ms"]),
        slot["opensearch_trimmed_avg_ms"] && format("%.3f", slot["opensearch_trimmed_avg_ms"])
      ]
    end
  end
end

def write_clickbench_artifacts(bundle_dir, samples_csv_path, benchmark_csv_path)
  slots = fixed_clickbench_slots(samples_csv_path, benchmark_csv_path)
  paths = {
    "slots_csv" => File.join(bundle_dir, "clickbench-slots.csv"),
    "times_json" => File.join(bundle_dir, "clickbench-times.json"),
    "kaldb_lines" => File.join(bundle_dir, "clickbench-kaldb-lines.txt"),
    "opensearch_lines" => File.join(bundle_dir, "clickbench-opensearch-lines.txt")
  }

  write_clickbench_slot_csv(paths.fetch("slots_csv"), slots)
  write_clickbench_line_file(paths.fetch("kaldb_lines"), slots, "kaldb_seconds")
  write_clickbench_line_file(paths.fetch("opensearch_lines"), slots, "opensearch_seconds")
  File.write(
    paths.fetch("times_json"),
    JSON.pretty_generate({
      "unit" => "seconds",
      "query_slots" => CLICKBENCH_QUERY_IDS.map { |query_id| "q#{query_id}" },
      "kaldb" => slots.map { |slot| slot["kaldb_seconds"].nil? || slot["kaldb_seconds"].empty? ? nil : slot["kaldb_seconds"] },
      "opensearch" => slots.map { |slot| slot["opensearch_seconds"].nil? || slot["opensearch_seconds"].empty? ? nil : slot["opensearch_seconds"] }
    }) + "\n")
  paths
end

def write_summary_markdown(path, metadata)
  summary = metadata["summary"]
  lines = []
  lines << "# Benchmark Run"
  lines << ""
  lines << "- Label: #{metadata["label"]}"
  lines << "- Run ID: #{metadata["run_id"]}"
  lines << "- Started: #{metadata["started_at"]}"
  lines << "- Finished: #{metadata["finished_at"]}"
  lines << "- Duration seconds: #{metadata["duration_seconds"]}"
  lines << "- Git branch: #{metadata.dig("git", "branch")}"
  lines << "- Git SHA: #{metadata.dig("git", "head_sha")}"
  lines << "- Dirty tree: #{metadata.dig("git", "dirty")}"
  lines << "- Lucene version: #{metadata.dig("versions", "lucene_version")}"
  lines << "- OpenSearch version: #{metadata.dig("versions", "opensearch_version")}"
  unless metadata["note"].to_s.empty?
    lines << "- Note: #{metadata["note"].gsub("\n", " | ")}"
  end
  lines << ""
  lines << "## JVM"
  lines << ""
  lines.concat(JvmCapture.summary_lines(metadata["jvm"]))
  lines << ""
  lines << "## Summary"
  lines << ""
  lines << "- Primary latency stat: median"
  lines << "- Trimmed average: excludes the minimum and maximum sample when there are at least 3 samples"
  lines << "- Total rows: #{summary["total_rows"]}"
  lines << "- Comparable rows: #{summary["comparable_rows"]}"
  lines << "- Unexpected mismatch rows: #{summary["mismatch_rows"]}"
  lines << "- Expected xfail rows: #{summary["expected_rows"]}"
  lines << "- KalDB failed rows: #{summary["astra_failed_rows"]}"
  lines << "- OpenSearch failed rows: #{summary["os_failed_rows"]}"
  lines << "- Expected KalDB failed rows: #{summary["expected_astra_failed_rows"]}"
  lines << "- Expected OpenSearch failed rows: #{summary["expected_os_failed_rows"]}"
  lines << "- KalDB faster rows: #{summary["astra_faster_rows"]}"
  lines << "- OpenSearch faster rows: #{summary["os_faster_rows"]}"
  lines << "- Ties: #{summary["tie_rows"]}"
  lines << ""
  lines << "## Worst Ratios"
  lines << ""
  summary["worst_ratios"].each do |row|
    lines << "- #{row["name"]} size=#{row["count"]}: kaldb=#{row["astra_ms"]}ms os=#{row["os_ms"]}ms ratio=#{format("%.2f", row["ratio"])}"
  end
  lines << ""
  lines << "## Best Ratios"
  lines << ""
  summary["best_ratios"].each do |row|
    lines << "- #{row["name"]} size=#{row["count"]}: kaldb=#{row["astra_ms"]}ms os=#{row["os_ms"]}ms ratio=#{format("%.2f", row["ratio"])}"
  end
  File.write(path, lines.join("\n") + "\n")
end

options = {
  iterations: 1,
  history_root: File.join(SCRIPT_DIR, "history", "runs"),
  label: "manual",
  note: ""
}

OptionParser.new do |parser|
  parser.banner = "Usage: ruby tracked_benchmark.rb [options]"
  parser.on("--label LABEL", "Short label for this run") { |value| options[:label] = value }
  parser.on("--note TEXT", "Free-form note for this run") { |value| options[:note] = value }
  parser.on("--note-file PATH", "Read note text from a file") { |value| options[:note_file] = value }
  parser.on("--iterations N", Integer, "Benchmark iterations (default: 1)") { |value| options[:iterations] = value }
  parser.on("--history-root PATH", "Where run bundles should be written") { |value| options[:history_root] = File.expand_path(value, SCRIPT_DIR) }
  parser.on("--run-id ID", "Override the generated run id") { |value| options[:run_id] = value }
end.parse!

started_at = Time.now
timestamp = started_at.strftime("%Y-%m-%d-%H-%M-%S")
run_id = options[:run_id] || "#{timestamp}-#{slugify(options[:label])}"
bundle_dir = File.join(options[:history_root], run_id)
output_dir = File.join(bundle_dir, "output")
results_file = File.join(bundle_dir, "benchmark.csv")
samples_file = File.join(bundle_dir, "samples.csv")
comparison_file = File.join(bundle_dir, "comparison.csv")
note = read_note(options)

if Dir.exist?(bundle_dir)
  abort("Run bundle already exists: #{bundle_dir}")
end

FileUtils.mkdir_p(bundle_dir)

metadata = {
  "run_id" => run_id,
  "label" => options[:label],
  "note" => note,
  "started_at" => started_at.iso8601,
  "benchmark_script" => File.join(SCRIPT_DIR, "benchmark.rb"),
  "iterations" => options[:iterations],
  "bundle_dir" => bundle_dir,
  "output_dir" => output_dir,
  "results_file" => results_file,
  "samples_file" => samples_file,
  "comparison_file" => comparison_file,
  "benchmark_environment" => benchmark_environment,
  "versions" => parse_versions,
  "git" => git_metadata,
  "system" => {
    "hostname" => Socket.gethostname,
    "ruby" => RUBY_DESCRIPTION,
    "ruby_path" => RbConfig.ruby
  },
  "jvm" => JvmCapture.write(bundle_dir)
}

File.write(File.join(bundle_dir, "run.json"), JSON.pretty_generate(metadata) + "\n")
File.write(File.join(bundle_dir, "git-status.txt"), metadata.dig("git", "status_short").to_s)
File.write(File.join(bundle_dir, "git-diff.patch"), metadata.dig("git", "diff").to_s)
File.write(File.join(bundle_dir, "git-diff-cached.patch"), metadata.dig("git", "diff_cached").to_s)

env = {
  "BENCHMARK_RUN_ID" => "output",
  "BENCHMARK_OUTPUT_ROOT" => bundle_dir,
  "BENCHMARK_SAMPLES_FILE" => samples_file,
  "BENCHMARK_RESULTS_FILE" => results_file
}

command = [RbConfig.ruby, File.join(SCRIPT_DIR, "benchmark.rb"), options[:iterations].to_s]
metadata["command"] = command
File.write(File.join(bundle_dir, "command.json"), JSON.pretty_generate(command) + "\n")

success = system(env, *command, chdir: SCRIPT_DIR)
finished_at = Time.now

metadata["finished_at"] = finished_at.iso8601
metadata["duration_seconds"] = (finished_at - started_at).round(2)
metadata["status"] = success ? "passed" : "failed"

if success && File.exist?(results_file)
  metadata["summary"] = summarize_csv(results_file)
  write_comparison_csv(comparison_file, results_file)
  metadata["clickbench_artifacts"] = write_clickbench_artifacts(bundle_dir, samples_file, results_file)
  if metadata.dig("summary", "failed_rows").to_i.positive? ||
      metadata.dig("summary", "mismatch_rows").to_i.positive?
    metadata["status"] = "failed"
  end
  write_summary_markdown(File.join(bundle_dir, "summary.md"), metadata)
end

File.write(File.join(bundle_dir, "run.json"), JSON.pretty_generate(metadata) + "\n")

index_path = File.join(SCRIPT_DIR, "history", "index.jsonl")
FileUtils.mkdir_p(File.dirname(index_path))
File.open(index_path, "a") do |file|
  file.puts(JSON.generate({
    "run_id" => metadata["run_id"],
    "label" => metadata["label"],
    "started_at" => metadata["started_at"],
    "finished_at" => metadata["finished_at"],
    "status" => metadata["status"],
    "bundle_dir" => metadata["bundle_dir"],
    "branch" => metadata.dig("git", "branch"),
    "head_sha" => metadata.dig("git", "head_sha"),
    "dirty" => metadata.dig("git", "dirty"),
    "lucene_version" => metadata.dig("versions", "lucene_version"),
    "opensearch_version" => metadata.dig("versions", "opensearch_version"),
    "summary" => metadata["summary"]
  }))
end

latest_link = File.join(SCRIPT_DIR, "history", "latest")
FileUtils.rm_f(latest_link)
FileUtils.ln_sf(bundle_dir, latest_link)

abort("Benchmark failed; see #{bundle_dir}") unless metadata["status"] == "passed"

puts "Recorded benchmark bundle at #{bundle_dir}"
