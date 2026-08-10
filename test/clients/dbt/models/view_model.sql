{{ config(materialized='view') }}
select 1 as id, 'alpha' as name
