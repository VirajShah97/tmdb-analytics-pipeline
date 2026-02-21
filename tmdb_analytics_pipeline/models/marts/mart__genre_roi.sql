{{ config(materialized='table') }}

with movie_genres as (

    select
        m.movie_id,
        m.title,
        m.release_year,
        m.budget,
        m.revenue,
        m.roi,
        m.profit,
        m.popularity,
        m.vote_average,
        m.vote_count,
        g.value:id::int         as genre_id,
        g.value:name::varchar   as genre_name

    from {{ ref('stg_movies') }} m,
    lateral flatten(input => m.genres_raw) g

    -- Only include movies where ROI is calculable
    where m.roi is not null

),

genre_aggregates as (

    select
        genre_id,
        genre_name,

        -- Volume
        count(distinct movie_id)            as total_movies,

        -- ROI metrics
        round(avg(roi), 4)                  as avg_roi,
        round(median(roi), 4)               as median_roi,
        round(min(roi), 4)                  as min_roi,
        round(max(roi), 4)                  as max_roi,

        -- Financial totals
        sum(budget)                         as total_budget,
        sum(revenue)                        as total_revenue,
        sum(profit)                         as total_profit,

        -- Portfolio ROI: treats all movies in a genre as one investment
        round(
            (sum(revenue) - sum(budget)) / nullif(sum(budget), 0)::float,
            4
        )                                   as portfolio_roi,

        -- Audience signals
        round(avg(popularity), 2)           as avg_popularity,
        round(avg(vote_average), 2)         as avg_vote_average,
        round(avg(vote_count), 0)           as avg_vote_count,

        -- Share of movies with positive ROI
        round(
            count(case when roi > 0 then 1 end) / count(*)::float,
            4
        )                                   as pct_profitable

    from movie_genres

    group by genre_id, genre_name

)

select
    genre_id,
    genre_name,
    total_movies,
    avg_roi,
    median_roi,
    min_roi,
    max_roi,
    portfolio_roi,
    total_budget,
    total_revenue,
    total_profit,
    avg_popularity,
    avg_vote_average,
    avg_vote_count,
    pct_profitable,

    -- Rank genres by average ROI
    rank() over (order by avg_roi desc)         as roi_rank,
    rank() over (order by portfolio_roi desc)   as portfolio_roi_rank

from genre_aggregates

/*
    Minimum threshold: exclude genres with fewer than 10 movies
    to avoid misleading ROI from small sample sizes
*/
where total_movies >= 10

order by avg_roi desc