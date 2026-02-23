{{ config(materialized='view') }}

with parsed as (

    select
        -- IDs & metadata
        raw_data:movie_id::int                        as movie_id,
        raw_data:title::varchar                 as title,
        raw_data:release_date::date             as release_date,
        raw_data:original_language::varchar     as original_language,
        raw_data:status::varchar                as status,

        -- Financials
        raw_data:budget::bigint                 as budget,
        raw_data:revenue::bigint                as revenue,

        -- Popularity signals
        raw_data:popularity::float              as popularity,
        raw_data:vote_average::float            as vote_average,
        raw_data:vote_count::int                as vote_count,
        raw_data:runtime::int                   as runtime_minutes,

        -- Genres stored as array: [{"id": 28, "name": "Action"}, ...]
        raw_data:genres                         as genres_raw,

        -- Load metadata
        raw_data:_ingested_at::timestamp_ntz    as ingested_at

    from {{ source('tmdb_raw', 'movies') }}

),

/*
    Remove duplicate movie records (Snowpipe appends on every Lambda run)
    Keep the most recently ingested copy of each movie
*/
deduplicated as (

    select *

    from parsed

    qualify row_number() over (
        partition by movie_id
        order by ingested_at desc
    ) = 1

),

final as (

    select
        movie_id,
        title,
        release_date,

        -- Derived date parts
        year(release_date)                      as release_year,
        month(release_date)                     as release_month,

        original_language,
        status,

        -- Null-safe financials
        nullif(budget, 0)                       as budget,
        nullif(revenue, 0)                      as revenue,

        -- ROI: only calculated when both budget and revenue are known and non-zero
        case
            when nullif(budget, 0) is not null
             and nullif(revenue, 0) is not null
            then round((revenue - budget) / budget::float, 4)
        end                                     as roi,

        -- Profit: revenue minus budget in absolute dollars
        case
            when nullif(budget, 0) is not null
             and nullif(revenue, 0) is not null
            then revenue - budget
        end                                     as profit,

        popularity,
        vote_average,
        vote_count,
        runtime_minutes,
        genres_raw,
        ingested_at

    from deduplicated

    /*
        Only keep movies with at least one financial data point
        (budget OR revenue present) so mart has usable data
    */
    where nullif(budget, 0) is not null
       or nullif(revenue, 0) is not null

)

select * from final