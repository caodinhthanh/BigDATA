with source_totals as (
    select
        review_month, app_id, sum(reviews) as reviews, sum(recommendations) as recommendations,
        sum(known_playtime) as known_playtime, sum(total_playtime_minutes) as total_playtime_minutes
    from {{ ref('stg_review_aggregates') }}
    group by review_month, app_id
),
mart_totals as (
    {% for mart in ['recommendation_trend', 'playtime_behavior', 'reviewer_activity'] %}
    select
        '{{ mart }}' as mart, review_month, app_id, sum(reviews) as reviews,
        sum(recommendations) as recommendations, sum(known_playtime) as known_playtime,
        sum(total_playtime_minutes) as total_playtime_minutes
    from {{ ref(mart) }}
    group by review_month, app_id
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
expected as (
    select mart, source_totals.*
    from source_totals
    cross join (values ('recommendation_trend'), ('playtime_behavior'), ('reviewer_activity')) as marts(mart)
)
(select * from expected except select * from mart_totals)
union all
(select * from mart_totals except select * from expected)
