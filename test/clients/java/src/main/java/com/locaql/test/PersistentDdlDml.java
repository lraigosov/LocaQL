package com.locaql.test;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Official com.google.cloud:google-cloud-bigquery smoke test for persistent
 * SQL mutations and streaming inserts, mirroring
 * test/clients/python/persistent_ddl_dml.py,
 * test/clients/node/persistent_ddl_dml.js and
 * test/clients/go/persistent_ddl_dml.go so every official client exercises
 * the same real server behavior.
 */
public final class PersistentDdlDml {

    private PersistentDdlDml() {}

    public static void main(String[] args) throws InterruptedException {
        String endpoint = "http://127.0.0.1:19050";
        for (String arg : args) {
            if (arg.startsWith("--endpoint=")) {
                endpoint = arg.substring("--endpoint=".length());
            }
        }

        // The official Java client's builder accepts a plain host override
        // (setHost) plus an explicit no-op credentials provider
        // (NoCredentials) for exactly this local-endpoint case — the same
        // convention the whole google-cloud-java family (Storage, Firestore,
        // Pub/Sub emulators) already uses, unlike the Node.js client, which
        // has no equivalent documented option.
        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setProjectId("p1")
                .setHost(endpoint)
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();

        runQuery(bigquery, "CREATE OR REPLACE TABLE analytics.java_sql_mutations (id INT64, name STRING, active BOOL)");

        long inserted = runDml(bigquery,
                "INSERT INTO analytics.java_sql_mutations (id, name, active) VALUES (1, 'one', TRUE), (2, NULL, FALSE)", 2);
        long updated = runDml(bigquery,
                "UPDATE analytics.java_sql_mutations SET name = 'two', active = TRUE WHERE id = 2", 1);

        runQuery(bigquery,
                "CREATE OR REPLACE TABLE analytics.java_sql_merge_source AS "
                        + "SELECT 2 AS id, 'two-merged' AS name, FALSE AS active UNION ALL SELECT 3, 'three', TRUE");
        long merged = runDml(bigquery,
                "MERGE INTO analytics.java_sql_mutations AS T "
                        + "USING analytics.java_sql_merge_source AS S ON T.id = S.id "
                        + "WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active "
                        + "WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, S.active)", 2);
        long deleted = runDml(bigquery, "DELETE FROM analytics.java_sql_mutations WHERE id = 1", 1);

        // Streaming insert (tabledata.insertAll) via the official
        // BigQuery.insertAll, not a query job.
        TableId tableId = TableId.of("analytics", "java_sql_mutations");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 4L);
        row.put("name", "four");
        row.put("active", true);
        InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(row).build());
        if (response.hasErrors()) {
            throw new RuntimeException("streaming insert failed: " + response.getInsertErrors());
        }

        TableResult result = bigquery.query(QueryJobConfiguration.newBuilder(
                "SELECT id, name, active FROM analytics.java_sql_mutations ORDER BY id").build());

        List<String> rows = new ArrayList<>();
        for (FieldValueList r : result.iterateAll()) {
            rows.add(String.format("{\"id\":%d,\"name\":\"%s\",\"active\":%b}",
                    r.get("id").getLongValue(), r.get("name").getStringValue(), r.get("active").getBooleanValue()));
        }

        System.out.println(String.format(
                "{\"delete\":%d,\"insert\":%d,\"merge\":%d,\"rows\":[%s],\"update\":%d}",
                deleted, inserted, merged, String.join(",", rows), updated));
    }

    private static Job runQuery(BigQuery bigquery, String sql) throws InterruptedException {
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql).build();
        Job job = bigquery.create(JobInfo.of(config));
        job = job.waitFor();
        if (job == null) {
            throw new RuntimeException("job no longer exists: " + sql);
        }
        if (job.getStatus().getError() != null) {
            throw new RuntimeException("job failed for " + sql + ": " + job.getStatus().getError());
        }
        return job;
    }

    private static long runDml(BigQuery bigquery, String sql, long expectedAffectedRows) throws InterruptedException {
        Job job = runQuery(bigquery, sql);
        JobStatistics.QueryStatistics stats = job.getStatistics();
        Long affected = stats.getNumDmlAffectedRows();
        if (affected == null || affected != expectedAffectedRows) {
            throw new RuntimeException("expected " + expectedAffectedRows + " affected rows for " + sql + ", got " + affected);
        }
        return affected;
    }
}
