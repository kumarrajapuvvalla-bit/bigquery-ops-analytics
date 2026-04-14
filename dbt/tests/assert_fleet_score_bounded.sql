-- Singular test: fleet health score must be in [0, 100].
select
    cluster_name,
    environment,
    event_hour,
    fleet_health_score
from {{ ref('int_fleet_health_scores') }}
where fleet_health_score < 0
   or fleet_health_score > 100
