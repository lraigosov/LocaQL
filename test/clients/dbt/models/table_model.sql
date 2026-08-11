{{ config(materialized='table') }}
select 1 as id, 'alpha' as name
union all
select 2 as id, 'beta' as name
