{{ config(materialized='incremental', unique_key='id', incremental_strategy='merge') }}
select * from (
  select 1 as id, 'alpha' as name, 100 as amount
  union all
  select 2 as id, 'beta' as name, 200 as amount
)
{% if is_incremental() %}
where id not in (select id from {{ this }})
{% endif %}
