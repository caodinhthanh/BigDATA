select
    review_month,
    app_id,
    playtime_band,
    activity_band,
    reviews,
    recommendations,
    known_playtime,
    total_playtime_minutes
from {{ source('warehouse', 'review_aggregates') }}
