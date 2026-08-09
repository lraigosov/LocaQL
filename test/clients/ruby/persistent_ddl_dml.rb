#!/usr/bin/env ruby
# frozen_string_literal: true

# Official google-cloud-bigquery smoke test for persistent SQL mutations and
# streaming inserts, mirroring test/clients/{python,node,go,java}/persistent_ddl_dml.*
# so every client exercises the same real server behavior.

require "json"
require "google/cloud/bigquery"

# The official gem has no documented anonymous-credentials mode for a custom
# local endpoint (unlike the Go/Java clients' AnonymousCredentials/
# NoCredentials). Google::Cloud::Bigquery.new only skips wrapping its
# credentials argument in Bigquery::Credentials (which requires a real
# keyfile) when the value passed already is_a? Google::Auth::Credentials —
# this subclass satisfies that check while never touching real credential
# material. service.authorization ends up nil, and google-apis-core's
# HttpCommand only attaches an Authorization header when
# options.authorization responds to :apply! or is a String (see
# google-apis-core/lib/google/apis/core/http_command.rb) — nil does neither,
# so requests go out with no Authorization header at all, matching how every
# other client in this suite talks to an anonymous-only local server.
class AnonymousCredentials < Google::Auth::Credentials
  def initialize; end

  def client
    nil
  end
end

def parse_endpoint(argv)
  endpoint = "http://127.0.0.1:19050"
  argv.each do |arg|
    match = /^--endpoint=(.+)$/.match(arg)
    endpoint = match[1] if match
  end
  endpoint
end

def new_client(endpoint)
  # google-apis-core builds request URLs as root_url + base_path + path, and
  # base_path is the fixed "bigquery/v2/" baked into BigqueryService — unlike
  # the Go client, `endpoint` here must be just the server root (with a
  # trailing slash), not the full ".../bigquery/v2/" path.
  root_url = endpoint.end_with?("/") ? endpoint : "#{endpoint}/"
  Google::Cloud::Bigquery.new(
    project_id: "p1",
    credentials: AnonymousCredentials.new,
    endpoint: root_url
  )
end

def run_query(bigquery, sql)
  job = bigquery.query_job sql
  job.wait_until_done!
  raise "query failed: #{sql.inspect}: #{job.error}" if job.failed?
  job
end

def run_dml(bigquery, sql, expected_affected_rows)
  job = run_query bigquery, sql
  affected = job.num_dml_affected_rows
  if affected != expected_affected_rows
    raise "expected #{expected_affected_rows} affected rows for #{sql.inspect}, got #{affected.inspect}"
  end
  expected_statement_type = sql.strip.split(/\s+/, 2).first.upcase
  if job.statement_type != expected_statement_type
    raise "expected statementType=#{expected_statement_type}, got #{job.statement_type.inspect}"
  end
  job
end

def main
  bigquery = new_client(parse_endpoint(ARGV))

  run_query bigquery, "CREATE OR REPLACE TABLE analytics.ruby_sql_mutations (id INT64, name STRING, active BOOL)"

  inserted = run_dml(
    bigquery,
    "INSERT INTO analytics.ruby_sql_mutations (id, name, active) VALUES (1, 'one', TRUE), (2, NULL, FALSE)",
    2
  )
  updated = run_dml(
    bigquery,
    "UPDATE analytics.ruby_sql_mutations SET name = 'two', active = TRUE WHERE id = 2",
    1
  )

  run_query(
    bigquery,
    "CREATE OR REPLACE TABLE analytics.ruby_sql_merge_source AS " \
    "SELECT 2 AS id, 'two-merged' AS name, FALSE AS active UNION ALL SELECT 3, 'three', TRUE"
  )
  merged = run_dml(
    bigquery,
    "MERGE INTO analytics.ruby_sql_mutations AS T " \
    "USING analytics.ruby_sql_merge_source AS S ON T.id = S.id " \
    "WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active " \
    "WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, S.active)",
    2
  )
  deleted = run_dml(bigquery, "DELETE FROM analytics.ruby_sql_mutations WHERE id = 1", 1)

  # Streaming insert (tabledata.insertAll) through the same table resource,
  # not a query job.
  table = bigquery.dataset("analytics").table("ruby_sql_mutations")
  insert_response = table.insert [{ id: 4, name: "four", active: true }]
  raise "streaming insert failed: #{insert_response.insert_errors}" unless insert_response.success?

  result = bigquery.query "SELECT id, name, active FROM analytics.ruby_sql_mutations ORDER BY id"
  got = result.map { |row| [row[:id], row[:name], row[:active]] }
  want = [[2, "two-merged", false], [3, "three", true], [4, "four", true]]
  raise "persistent DML + streaming insert rows mismatch: #{got.inspect}" if got != want

  run_query bigquery, "DROP TABLE analytics.ruby_sql_merge_source"

  puts JSON.generate(
    {
      delete: deleted.num_dml_affected_rows,
      insert: inserted.num_dml_affected_rows,
      merge: merged.num_dml_affected_rows,
      rows: got,
      update: updated.num_dml_affected_rows
    }
  )
end

main
