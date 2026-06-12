#!/usr/bin/env ruby

require "json"

module ClickBenchQueries
  SCRIPT_DIR = File.expand_path(__dir__)
  DEFAULT_QUERY_DIR = File.join(SCRIPT_DIR, "clickbench", "queries")

  module_function

  def default_profile
    "checked-in-clickbench-json"
  end

  def load(query_dir = DEFAULT_QUERY_DIR)
    paths = Dir.glob(File.join(query_dir, "q*.json")).sort
    raise "No ClickBench query JSON files found in #{query_dir}" if paths.empty?

    queries = paths.map do |path|
      basename = File.basename(path, ".json")
      match = basename.match(/\Aq(\d+)\z/)
      raise "Unexpected ClickBench query filename: #{path}" unless match

      query_number = Integer(match[1], 10)
      {
        name: "q#{query_number}",
        query_number: query_number,
        request: JSON.parse(File.read(path))
      }
    end

    duplicate_numbers = queries.group_by { |query| query[:query_number] }.select { |_, values| values.size > 1 }.keys
    raise "Duplicate ClickBench query numbers: #{duplicate_numbers.join(", ")}" unless duplicate_numbers.empty?

    queries.sort_by { |query| query[:query_number] }
  end
end
