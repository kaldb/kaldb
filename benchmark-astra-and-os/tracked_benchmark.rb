#!/usr/bin/env ruby

require "csv"
require "fileutils"
require "json"
require "open3"
require "optparse"
require "rbconfig"
require "socket"
require "time"

SCRIPT_DIR = File.expand_path(__dir__)
REPO_ROOT = File.expand_path("..", SCRIPT_DIR)

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

def summarize_csv(csv_path)
  rows = CSV.read(csv_path, headers: true)
  comparable_rows = []
  mismatch_rows = []
  astra_failed_rows = 0
  os_failed_rows = 0

  rows.each do |row|
    notes = parse_notes_field(row["comparison_notes"])
    astra_failed_rows += 1 if row["astra_asterisk"] == "true"
    os_failed_rows += 1 if row["os_asterisk"] == "true"
    entry = {
      "name" => row["name"],
      "count" => row["count"].to_i,
      "astra_ms" => row["astra_ms"].to_f,
      "os_ms" => row["os_ms"].to_f,
      "notes" => notes
    }
    if notes.empty?
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
    "astra_failed_rows" => astra_failed_rows,
    "os_failed_rows" => os_failed_rows,
    "failed_rows" => [astra_failed_rows, os_failed_rows].max,
    "astra_faster_rows" => astra_faster,
    "os_faster_rows" => os_faster,
    "tie_rows" => ties,
    "worst_ratios" => ratio_rows.sort_by { |row| -row["ratio"] }.first(5),
    "best_ratios" => ratio_rows.sort_by { |row| row["ratio"] }.first(5)
  }
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
  lines << "## Summary"
  lines << ""
  lines << "- Total rows: #{summary["total_rows"]}"
  lines << "- Comparable rows: #{summary["comparable_rows"]}"
  lines << "- Mismatch rows: #{summary["mismatch_rows"]}"
  lines << "- Astra failed rows: #{summary["astra_failed_rows"]}"
  lines << "- OpenSearch failed rows: #{summary["os_failed_rows"]}"
  lines << "- Astra faster rows: #{summary["astra_faster_rows"]}"
  lines << "- OpenSearch faster rows: #{summary["os_faster_rows"]}"
  lines << "- Ties: #{summary["tie_rows"]}"
  lines << ""
  lines << "## Worst Ratios"
  lines << ""
  summary["worst_ratios"].each do |row|
    lines << "- #{row["name"]} size=#{row["count"]}: astra=#{row["astra_ms"]}ms os=#{row["os_ms"]}ms ratio=#{format("%.2f", row["ratio"])}"
  end
  lines << ""
  lines << "## Best Ratios"
  lines << ""
  summary["best_ratios"].each do |row|
    lines << "- #{row["name"]} size=#{row["count"]}: astra=#{row["astra_ms"]}ms os=#{row["os_ms"]}ms ratio=#{format("%.2f", row["ratio"])}"
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
  "versions" => parse_versions,
  "git" => git_metadata,
  "system" => {
    "hostname" => Socket.gethostname,
    "ruby" => RUBY_DESCRIPTION,
    "ruby_path" => RbConfig.ruby
  }
}

File.write(File.join(bundle_dir, "run.json"), JSON.pretty_generate(metadata) + "\n")
File.write(File.join(bundle_dir, "git-status.txt"), metadata.dig("git", "status_short").to_s)
File.write(File.join(bundle_dir, "git-diff.patch"), metadata.dig("git", "diff").to_s)
File.write(File.join(bundle_dir, "git-diff-cached.patch"), metadata.dig("git", "diff_cached").to_s)

env = {
  "BENCHMARK_RUN_ID" => "output",
  "BENCHMARK_OUTPUT_ROOT" => bundle_dir,
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
  if metadata.dig("summary", "failed_rows").to_i.positive?
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
