-- Singular test: no fleet events should have a timestamp in the future.
-- Fails if any row is returned.
select
    event_id,
    event_ts
from {{ ref('stg_fleet_events') }}
where event_ts > current_timestamp()
