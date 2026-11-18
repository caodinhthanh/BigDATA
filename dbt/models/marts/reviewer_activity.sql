select
    review_month,
    app_id,
    activity_band,
    sum(reviews)::bigint as reviews,
    sum(recommendations)::bigint as recommendations,
    sum(recommendations)::numeric / nullif(sum(reviews), 0) as recommendation_rate,
    sum(known_playtime)::bigint as known_playtime,
    sum(total_playtime_minutes) as total_playtime_minutes,
    sum(total_playtime_minutes) / nullif(sum(known_playtime), 0) / 60 as avg_playtime_hours
from {{ ref('stg_review_aggregates') }}
group by review_month, app_id, activity_band
