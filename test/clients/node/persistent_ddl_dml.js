#!/usr/bin/env node
'use strict';

// Official @google-cloud/bigquery smoke test for persistent SQL mutations
// and streaming inserts, mirroring test/clients/python/persistent_ddl_dml.py
// so both clients exercise the same real server behavior.

const {BigQuery} = require('@google-cloud/bigquery');
const {util} = require('@google-cloud/common');

function parseArgs() {
  const args = {endpoint: 'http://127.0.0.1:19050'};
  for (const arg of process.argv.slice(2)) {
    const match = /^--endpoint=(.+)$/.exec(arg);
    if (match) {
      args.endpoint = match[1];
    }
  }
  return args;
}

// The official client has no documented anonymous-credentials mode for a
// custom local endpoint (unlike the Python/Go clients' AnonymousCredentials).
// @google-cloud/common's own request factory explicitly supports this case —
// its authorizeRequest skips google-auth-library entirely when
// `customEndpoint` is set and `useAuthWithCustomEndpoint` is not (see its
// source: "Using a custom API override. Do not use google-auth-library for
// authentication. (ex: connecting to a local Datastore server)") — but the
// BigQuery constructor does not forward `customEndpoint` into that factory
// itself, so the request factory has to be rebuilt and reassigned after
// construction with the same options.
function newClient(endpoint) {
  const options = {
    projectId: 'p1',
    apiEndpoint: endpoint,
    scopes: ['https://www.googleapis.com/auth/bigquery'],
    customEndpoint: true,
  };
  const bigquery = new BigQuery(options);
  bigquery.makeAuthenticatedRequest = util.makeAuthenticatedRequestFactory(options);
  return bigquery;
}

async function runQuery(bigquery, sql) {
  const [job] = await bigquery.createQueryJob({query: sql});
  await job.getQueryResults();
  const [metadata] = await job.getMetadata();
  return metadata;
}

async function runDml(bigquery, sql, expectedAffectedRows) {
  const metadata = await runQuery(bigquery, sql);
  const stats = metadata.statistics && metadata.statistics.query;
  const affected = stats ? Number(stats.numDmlAffectedRows) : NaN;
  if (affected !== expectedAffectedRows) {
    throw new Error(
      `expected ${expectedAffectedRows} affected rows for ${JSON.stringify(sql)}, got ${affected} (metadata: ${JSON.stringify(stats)})`
    );
  }
  const expectedStatementType = sql.trim().split(/\s+/, 1)[0].toUpperCase();
  if (stats.statementType !== expectedStatementType) {
    throw new Error(`expected statementType=${expectedStatementType}, got ${stats.statementType}`);
  }
  return metadata;
}

async function main() {
  const {endpoint} = parseArgs();
  const bigquery = newClient(endpoint);

  await runQuery(
    bigquery,
    'CREATE OR REPLACE TABLE analytics.node_sql_mutations (id INT64, name STRING, active BOOL)'
  );

  const inserted = await runDml(
    bigquery,
    "INSERT INTO analytics.node_sql_mutations (id, name, active) VALUES (1, 'one', TRUE), (2, NULL, FALSE)",
    2
  );
  const updated = await runDml(
    bigquery,
    "UPDATE analytics.node_sql_mutations SET name = 'two', active = TRUE WHERE id = 2",
    1
  );

  await runQuery(
    bigquery,
    'CREATE OR REPLACE TABLE analytics.node_sql_merge_source AS ' +
      "SELECT 2 AS id, 'two-merged' AS name, FALSE AS active UNION ALL SELECT 3, 'three', TRUE"
  );
  const merged = await runDml(
    bigquery,
    'MERGE INTO analytics.node_sql_mutations AS T ' +
      'USING analytics.node_sql_merge_source AS S ON T.id = S.id ' +
      'WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active ' +
      'WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, S.active)',
    2
  );
  const deleted = await runDml(bigquery, 'DELETE FROM analytics.node_sql_mutations WHERE id = 1', 1);

  // Streaming insert (tabledata.insertAll) through the same table resource,
  // not a query job — real BigQuery clients use table.insert() for this.
  await bigquery.dataset('analytics').table('node_sql_mutations').insert([{id: 4, name: 'four', active: true}]);

  const [rows] = await bigquery.query({
    query: 'SELECT id, name, active FROM analytics.node_sql_mutations ORDER BY id',
  });
  const got = rows.map((r) => [Number(r.id), r.name, r.active]);
  const want = [
    [2, 'two-merged', false],
    [3, 'three', true],
    [4, 'four', true],
  ];
  if (JSON.stringify(got) !== JSON.stringify(want)) {
    throw new Error(`persistent DML + streaming insert rows mismatch: ${JSON.stringify(got)}`);
  }

  await runQuery(bigquery, 'DROP TABLE analytics.node_sql_merge_source');

  console.log(
    JSON.stringify({
      delete: Number(deleted.statistics.query.numDmlAffectedRows),
      insert: Number(inserted.statistics.query.numDmlAffectedRows),
      merge: Number(merged.statistics.query.numDmlAffectedRows),
      rows: got,
      update: Number(updated.statistics.query.numDmlAffectedRows),
    })
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
