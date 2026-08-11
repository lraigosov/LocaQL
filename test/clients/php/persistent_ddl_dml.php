<?php
/**
 * Official google/cloud-bigquery smoke test for persistent SQL mutations and
 * streaming inserts, mirroring
 * test/clients/{python,node,go,java,ruby}/persistent_ddl_dml.* so every
 * official client exercises the same real server behavior.
 *
 * The official PHP client has no documented anonymous-credentials mode for a
 * custom local endpoint (unlike the Go/Java clients' AnonymousCredentials/
 * NoCredentials). BigQueryClient's constructor accepts a `credentialsFetcher`
 * option typed `Google\Auth\FetchAuthTokenInterface` — confirmed by reading
 * the installed source (vendor/google/cloud-bigquery/src/BigQueryClient.php),
 * not documentation — which is a plain 3-method interface with no real OAuth
 * machinery behind it. A fetcher returning a literal, never-refreshed fake
 * token satisfies it without touching real credential material, the same
 * "fake bearer token nobody validates" approach already used for dbt/bq.
 */

require __DIR__ . '/vendor/autoload.php';

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\BigQuery\BigQueryClient;

final class FakeTokenCredentials implements FetchAuthTokenInterface
{
    public function fetchAuthToken(?callable $httpHandler = null)
    {
        return ['access_token' => 'locaql-fake-token', 'expires_in' => 3600];
    }

    public function getCacheKey()
    {
        return 'locaql-fake-token';
    }

    public function getLastReceivedToken()
    {
        return ['access_token' => 'locaql-fake-token', 'expires_at' => time() + 3600];
    }
}

function parse_endpoint(array $argv): string
{
    $endpoint = 'http://127.0.0.1:19050';
    foreach ($argv as $arg) {
        if (str_starts_with($arg, '--endpoint=')) {
            $endpoint = substr($arg, strlen('--endpoint='));
        }
    }
    return $endpoint;
}

function new_client(string $endpoint): BigQueryClient
{
    // RestTrait::getApiEndpoint only defaults to the https:// scheme when
    // the given value contains no "//" at all (confirmed reading the
    // installed source) — passing the endpoint through with its scheme
    // intact, rather than stripping it, is what keeps this on plain HTTP.
    return new BigQueryClient([
        'projectId' => 'p1',
        'apiEndpoint' => $endpoint,
        'credentialsFetcher' => new FakeTokenCredentials(),
    ]);
}

function run_query(BigQueryClient $bigQuery, string $sql)
{
    $results = $bigQuery->runQuery($bigQuery->query($sql));
    $job = $results->job();
    $job->reload();
    $info = $job->info();
    if (($info['status']['state'] ?? null) !== 'DONE') {
        throw new RuntimeException("query did not complete: $sql");
    }
    if (isset($info['status']['errorResult'])) {
        throw new RuntimeException("query failed: $sql: " . json_encode($info['status']['errorResult']));
    }
    return $info;
}

function run_dml(BigQueryClient $bigQuery, string $sql, int $expectedAffectedRows): int
{
    $info = run_query($bigQuery, $sql);
    $stats = $info['statistics']['query'] ?? [];
    $affected = (int) ($stats['numDmlAffectedRows'] ?? -1);
    if ($affected !== $expectedAffectedRows) {
        throw new RuntimeException(
            "expected $expectedAffectedRows affected rows for $sql, got $affected"
        );
    }
    $expectedStatementType = strtoupper(strtok(trim($sql), " \t\n"));
    $statementType = $stats['statementType'] ?? null;
    if ($statementType !== $expectedStatementType) {
        throw new RuntimeException(
            "expected statementType=$expectedStatementType, got " . var_export($statementType, true)
        );
    }
    return $affected;
}

function main(array $argv): void
{
    $bigQuery = new_client(parse_endpoint($argv));

    run_query($bigQuery, 'CREATE OR REPLACE TABLE analytics.php_sql_mutations (id INT64, name STRING, active BOOL)');

    $inserted = run_dml(
        $bigQuery,
        "INSERT INTO analytics.php_sql_mutations (id, name, active) VALUES (1, 'one', TRUE), (2, NULL, FALSE)",
        2
    );
    $updated = run_dml(
        $bigQuery,
        "UPDATE analytics.php_sql_mutations SET name = 'two', active = TRUE WHERE id = 2",
        1
    );

    run_query(
        $bigQuery,
        'CREATE OR REPLACE TABLE analytics.php_sql_merge_source AS ' .
        "SELECT 2 AS id, 'two-merged' AS name, FALSE AS active UNION ALL SELECT 3, 'three', TRUE"
    );
    $merged = run_dml(
        $bigQuery,
        'MERGE INTO analytics.php_sql_mutations AS T ' .
        'USING analytics.php_sql_merge_source AS S ON T.id = S.id ' .
        'WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active ' .
        'WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, S.active)',
        2
    );
    $deleted = run_dml($bigQuery, 'DELETE FROM analytics.php_sql_mutations WHERE id = 1', 1);

    // Streaming insert (tabledata.insertAll) through the same table
    // resource, not a query job.
    $table = $bigQuery->dataset('analytics')->table('php_sql_mutations');
    $insertResponse = $table->insertRow(['id' => 4, 'name' => 'four', 'active' => true]);
    if (!$insertResponse->isSuccessful()) {
        throw new RuntimeException('streaming insert failed: ' . json_encode($insertResponse->failedRows()));
    }

    $results = $bigQuery->runQuery(
        $bigQuery->query('SELECT id, name, active FROM analytics.php_sql_mutations ORDER BY id')
    );
    $got = [];
    foreach ($results as $row) {
        $got[] = [$row['id'], $row['name'], $row['active']];
    }
    $want = [[2, 'two-merged', false], [3, 'three', true], [4, 'four', true]];
    if ($got !== $want) {
        throw new RuntimeException('persistent DML + streaming insert rows mismatch: ' . json_encode($got));
    }

    run_query($bigQuery, 'DROP TABLE analytics.php_sql_merge_source');

    echo json_encode([
        'delete' => $deleted,
        'insert' => $inserted,
        'merge' => $merged,
        'rows' => $got,
        'update' => $updated,
    ]) . "\n";
}

main($argv);
