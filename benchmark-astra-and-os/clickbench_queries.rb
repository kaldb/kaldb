#!/usr/bin/env ruby

require "json"

module ClickBenchQueries
  SCRIPT_DIR = File.expand_path(__dir__)
  REPO_ROOT = File.expand_path("..", SCRIPT_DIR)
  DEFAULT_TEST_PATH =
    File.join(REPO_ROOT, "astra/src/test/java/com/slack/astra/server/ClickBenchCompatibilityTest.java")

  module_function

  def load(test_path = DEFAULT_TEST_PATH)
    source = File.read(test_path)
    queries = []
    offset = 0

    while (match = source.match(/clickBench\("Q(\d+)"\)/, offset))
      query_number = Integer(match[1])
      request_call = source.index("whenAstraReceivesEquivalentOpenSearch", match.begin(0))
      raise "Q#{query_number} has no request JSON" unless request_call

      text_start = source.index('"""', request_call)
      text_end = source.index('"""', text_start + 3)
      raise "Q#{query_number} has an unterminated request JSON text block" unless text_end

      request_json = source[(text_start + 3)...text_end].strip
      queries << {
        name: "q#{query_number}",
        query_number: query_number,
        request: JSON.parse(request_json)
      }
      offset = text_end + 3
    end

    queries.sort_by { |query| query.fetch(:query_number) }
  end
end
