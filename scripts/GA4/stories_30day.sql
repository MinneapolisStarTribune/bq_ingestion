 IF (
    EXISTS( 
        SELECT 1 
        FROM `daring-phoenix-295916.analytics_248462478.events_*` 
        WHERE _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)) )) 
        AND (NOT EXISTS( 
            SELECT creation_time, table_name 
            FROM `daring-phoenix-295916.custom_reporting.INFORMATION_SCHEMA.TABLES` 
            WHERE table_name = 'ga4_stories_30_day' AND date(creation_time) = CURRENT_DATE('America/Chicago') )) 
        THEN CREATE OR REPLACE TABLE `custom_reporting.ga4_stories_30_day` AS 
        WITH pageviews AS( 
            SELECT
    (
        SELECT
            value.int_value
        FROM
            UNNEST (event_params)
        WHERE
            key = 'article_id'
    ) as article_id, 
    -- (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'article_enforcement_level') as article_enforcement_level, 
    COUNT(*) as pageviews, 
    COUNT(DISTINCT concat(user_pseudo_id, " ", (SELECT value.int_value FROM unnest(event_params) WHERE key = "ga_session_id") )) as sessions, 
    COUNT(DISTINCT user_pseudo_id) as users 
    FROM `daring-phoenix-295916.analytics_248462478.events_*` 
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 30 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)) 
    AND event_name = 'page_view' 
    AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "https://stage") 
    AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "https://prod-") 
    AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "https://rebrand-") 
    AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "localhost") 
    AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "vercel") 
    AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'publish_date'), ":") GROUP BY 1 ), 
    
    meta_data AS ( 
        SELECT 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_name') as page_name, 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') as time_published, 
            CASE 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "12 am" then 0 
                
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "1 am" then 1 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "2 am" then 2 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "3 am" then 3 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "4 am" then 4 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "5 am" then 5 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "6 am" then 6 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "7 am" then 7 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "8 am" then 8 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "9 am" then 9 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "10 am" then 10 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "11 am" then 11 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "12 pm" then 12 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "1 pm" then 13 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "2 pm" then 14 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "3 pm" then 15 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "4 pm" then 16 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "5 pm" then 17 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "6 pm" then 18 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "7 pm" then 19 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "8 pm" then 20 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "9 pm" then 21 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "10 pm" then 22 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'time_published') = "11 pm" then 23 
            END as time_published_24hr, 
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'article_id') as article_id, 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'author') as author, 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'publish_date') as publish_date, 
            CASE 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'publish_date') = "no publish date" then "no publish date" 
                WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'publish_date') = "invalid date" THEN "invalid date" 
                ELSE FORMAT_DATE( '%A',PARSE_DATE("%B %d, %Y", INITCAP((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'publish_date'))) ) 
            END AS publish_dow, 
            REPLACE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_section'), 'channel=', '') as page_section, 
            REPLACE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'subsection'), 'c7=', '') as subsection, 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_level') as page_level, 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content_source') as content_source, 
            -- (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'article_enforcement_level') as article_enforcement_level, 
            -- (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'article_enforcement_date') as article_enforcement_date, 
            REGEXP_EXTRACT(REPLACE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "m.", "www."), "^[^?]+") as full_url, 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'all_sections') as all_sections, 
            COUNT(*) as pageviews 
            FROM `daring-phoenix-295916.analytics_248462478.events_*` 
            WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 30 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)) 
            AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_level') = 'story' 
            AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'subsection') != 'comments' 
            AND -- (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content_source') = 'star tribune' 
            AND event_name = 'page_view' 
            AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "https://stage") 
            AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "https://prod-") 
            AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "https://rebrand-") 
            AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "localhost") 
            AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url'), "vercel") 
            AND NOT contains_substr((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'publish_date'), ":") 
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13 ),
            
        most as (
    SELECT
        t.article_id,
        t.full_url,
        t.author,
        t.publish_date,
        t.publish_dow,
        t.time_published,
        t.page_section,
        t.all_sections,
        t.subsection,
        t.page_level,
        t.content_source,
        p.pageviews,
        p.sessions,
        p.users,
        t.time_published_24hr
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        article_id
                    ORDER BY
                        pageviews DESC
                ) rn
            FROM
                meta_data
        ) t
        LEFT JOIN pageviews p ON t.article_id = p.article_id
    WHERE
        t.rn = 1
)
SELECT
    m.*,
    md.page_name
FROM
    (
        SELECT
            page_name,
            article_id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    article_id
                ORDER BY
                    pageviews DESC
            ) rn
        FROM
            meta_data
    ) md
    LEFT JOIN most m ON m.article_id = md.article_id
WHERE
    md.rn = 1
ORDER BY
    pageviews DESC;

END IF;