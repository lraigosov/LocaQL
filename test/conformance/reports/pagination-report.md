# Pagination Conformance Report

- Suite: pagination
- Started at: 2026-07-19T20:11:43Z
- Base URL: http://localhost:9050
- Passed: 4
- Failed: 0

| Case | Method | Path | Expected | Actual | Result |
|---|---|---|---:|---:|---|
| rest_datasets_list_pagination | GET | /bigquery/v2/projects/p1/datasets?maxResults=2 | 200 | 200 | PASS |
| rest_tables_list_pagination | GET | /bigquery/v2/projects/p1/datasets/ds1/tables?maxResults=2 | 200 | 200 | PASS |
| rest_jobs_list_pagination | GET | /bigquery/v2/projects/p1/jobs?maxResults=2&pageToken=2 | 200 | 200 | PASS |
| rest_tabledata_list_pagination | GET | /bigquery/v2/projects/p1/tabledata/ds1/t1/data?startIndex=1&maxResults=2 | 200 | 200 | PASS |
