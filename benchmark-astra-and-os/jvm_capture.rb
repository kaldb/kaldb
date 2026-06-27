require "json"
require "open3"
require "time"

module JvmCapture
  SUBJECT_CONTAINERS = {
    "astra" => "astra_single",
    "kaldb" => "astra_single",
    "os" => "dep_opensearch",
    "opensearch" => "dep_opensearch"
  }.freeze

  ENV_PATTERN = /JAVA|JVM|HEAP|GC|OPENSEARCH|ASTRA/i
  LOG_PATTERN =
    /version\[|JVM home|JVM arguments|heap size|UseG1GC|MaxDirectMemory|Picked up JAVA_TOOL_OPTIONS|java\.version|java\.home/i
  KEY_FLAGS =
    %w[
      AlwaysPreTouch
      ConcGCThreads
      G1HeapRegionSize
      G1ReservePercent
      InitialHeapSize
      InitiatingHeapOccupancyPercent
      MaxDirectMemorySize
      MaxHeapSize
      ParallelGCThreads
      SoftMaxHeapSize
      ThreadStackSize
      UseCompressedOops
      UseG1GC
      UseStringDeduplication
    ].freeze
  KEY_FLAGS_PATTERN = Regexp.union(KEY_FLAGS)

  module_function

  def run_cmd(*cmd)
    stdout, stderr, status = Open3.capture3(*cmd)
    {
      "cmd" => cmd,
      "stdout" => stdout,
      "stderr" => stderr,
      "success" => status.success?,
      "exit_status" => status.exitstatus
    }
  rescue SystemCallError => e
    {
      "cmd" => cmd,
      "stdout" => "",
      "stderr" => e.message,
      "success" => false,
      "exit_status" => nil
    }
  end

  def container_subjects(subjects)
    subjects.map { |subject| subject.to_s.downcase }
      .map { |subject| SUBJECT_CONTAINERS.key?(subject) ? subject : nil }
      .compact
      .uniq
  end

  def capture(subjects: %w[astra os])
    selected_subjects = container_subjects(subjects)
    {
      "captured_at" => Time.now.utc.iso8601,
      "containers" =>
        selected_subjects.to_h do |subject|
          [subject, capture_container(SUBJECT_CONTAINERS.fetch(subject))]
        end
    }
  end

  def write(run_dir, subjects: %w[astra os])
    data = capture(subjects: subjects)
    File.write(File.join(run_dir, "jvm.json"), JSON.pretty_generate(data) + "\n")
    File.write(File.join(run_dir, "jvm-summary.txt"), summary_lines(data).join("\n") + "\n")
    data
  end

  def capture_container(container_name)
    inspected = inspect_container(container_name)
    return inspected unless inspected["exists"]

    container = inspected.fetch("inspect")
    running = container.dig("State", "Running") == true
    captured = {
      "exists" => true,
      "container_name" => container_name,
      "image" => container.dig("Config", "Image"),
      "state" => {
        "status" => container.dig("State", "Status"),
        "running" => running,
        "started_at" => container.dig("State", "StartedAt"),
        "finished_at" => container.dig("State", "FinishedAt"),
        "exit_code" => container.dig("State", "ExitCode")
      },
      "jvm_related_env" => jvm_related_env(container)
    }

    if running
      captured["runtime"] = capture_running_runtime(container_name)
    else
      captured["recent_jvm_log_lines"] = recent_jvm_log_lines(container_name)
    end
    captured
  end

  def inspect_container(container_name)
    inspected = run_cmd("docker", "inspect", container_name)
    unless inspected["success"]
      return {
        "exists" => false,
        "container_name" => container_name,
        "error" => inspected["stderr"].strip
      }
    end

    {
      "exists" => true,
      "inspect" => JSON.parse(inspected["stdout"]).first
    }
  rescue JSON::ParserError => e
    {
      "exists" => false,
      "container_name" => container_name,
      "error" => "docker inspect JSON parse failed: #{e.message}"
    }
  end

  def jvm_related_env(container)
    Array(container.dig("Config", "Env"))
      .select { |value| value.match?(ENV_PATTERN) }
      .sort
  end

  def capture_running_runtime(container_name)
    {
      "java_version" => docker_exec(container_name, "java -version 2>&1"),
      "process_cmdline" => docker_exec(container_name, "tr '\\0' ' ' < /proc/1/cmdline 2>/dev/null || true"),
      "jcmd" => docker_exec(container_name, jcmd_script),
      "recent_jvm_log_lines" => recent_jvm_log_lines(container_name)
    }.tap do |runtime|
      runtime["selected_jvm_flags"] = selected_jvm_flags(runtime.dig("jcmd", "stdout"))
    end
  end

  def docker_exec(container_name, command)
    run_cmd("docker", "exec", container_name, "sh", "-lc", command)
  end

  def jcmd_script
    <<~SH
      if command -v jcmd >/dev/null 2>&1; then
        jcmd 1 VM.version 2>&1
        jcmd 1 VM.command_line 2>&1
        jcmd 1 VM.flags 2>&1
        echo "--- selected VM.flags -all ---"
        jcmd 1 VM.flags -all 2>&1 | grep -E '#{KEY_FLAGS.join("|")}' || true
      else
        echo "jcmd not found"
      fi
    SH
  end

  def selected_jvm_flags(output)
    output.to_s.each_line
      .map(&:strip)
      .select { |line| line.match?(KEY_FLAGS_PATTERN) }
  end

  def recent_jvm_log_lines(container_name)
    logs = run_cmd("docker", "logs", "--tail", "500", container_name)
    text = [logs["stdout"], logs["stderr"]].join("\n")
    text.each_line.map(&:strip).select { |line| line.match?(LOG_PATTERN) }.uniq
  end

  def summary_lines(data)
    lines = []
    lines << "Full JVM/container capture: jvm.json"
    data.fetch("containers", {}).each do |subject, container|
      lines << "- #{subject_label(subject)} (#{container["container_name"]}): #{container.dig("state", "status") || "missing"}"
      lines << "  image: #{container["image"]}" if container["image"]
      env = container.fetch("jvm_related_env", [])
      lines << "  env: #{env.join("; ")}" unless env.empty?
      java_line = first_java_version_line(container)
      lines << "  java: #{java_line}" if java_line
      heap_line = first_log_line(container, /heap size/i)
      lines << "  heap log: #{heap_line}" if heap_line
      flags = container.dig("runtime", "selected_jvm_flags").to_a
      lines << "  selected flags: #{flags.join("; ")}" unless flags.empty?
      log_args = first_log_line(container, /JVM arguments/i)
      lines << "  log JVM arguments: #{log_args}" if log_args
    end
    lines
  end

  def subject_label(subject)
    subject == "astra" ? "KalDB" : "OpenSearch"
  end

  def first_java_version_line(container)
    output = [
      container.dig("runtime", "java_version", "stdout"),
      container.dig("runtime", "java_version", "stderr")
    ].join("\n")
    output.each_line.map(&:strip).find { |line| line.match?(/openjdk version|java version|OpenJDK/i) } ||
      first_log_line(container, /version\[/i)
  end

  def first_log_line(container, pattern)
    lines = container.dig("runtime", "recent_jvm_log_lines") || container["recent_jvm_log_lines"] || []
    lines.find { |line| line.match?(pattern) }
  end
end
