-- Insert into PTC.pages if data is available for yesterday and table hasn't updated today 
IF (EXISTS( 
    SELECT 1 
    FROM `daring-phoenix-295916.analytics_248462478.events_*` 
    WHERE _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)) )) 
    AND (NOT EXISTS( SELECT 1 FROM `daring-phoenix-295916.PTC.user_id` WHERE purchase_date = FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)) )) 
    THEN INSERT INTO `PTC.user_id` (purchase_date, days_before_purchase, article_id, section, page_level, user_id) 
        SELECT purchase_date, days_before_purchase, article_id, section, page_level, user_id FROM ( 
    -- Define purchase events made yesterday 
 WITH
    purchase_ids AS (
        SELECT
            FORMAT_DATE ("%Y-%m-%d", PARSE_DATE ("%Y%m%d", event_date)) AS _date,
            (
                SELECT value.int_value FROM UNNEST (event_params) WHERE key = 'dti_id'
            ) AS sub_id,
            CONCAT (
                user_pseudo_id,
                (
                    SELECT
                        value.int_value
                    FROM
                        UNNEST (event_params)
                    WHERE
                        key = 'ga_session_id'
                )
            ) AS session_id,
            user_pseudo_id,
            event_timestamp AS purchase_timestamp
        FROM
            `daring-phoenix-295916.analytics_248462478.events_*`
        WHERE
            _TABLE_SUFFIX = FORMAT_DATE (
                "%Y%m%d",
                DATE_SUB (CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            )
            AND event_name = "purchase_complete"
    ), 
    -- Define page view events in the last 30 days 
    pages AS ( SELECT user_pseudo_id, 
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'article_id') AS STRING) AS article_id, 
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_section') AS section, 
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_level') AS page_level, 
    event_timestamp AS page_view_timestamp 
    FROM `daring-phoenix-295916.analytics_248462478.events_*` 
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 30 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)) 
    AND event_name = "page_view" AND 
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'dti_id') IS NULL ), 
    -- Calculate days before purchase for each user-article combination 
raw AS (
    SELECT
        i._date AS purchase_date,
        i.user_pseudo_id,
        TIMESTAMP_DIFF (
            TIMESTAMP_MICROS (p.page_view_timestamp),
            TIMESTAMP_MICROS (i.purchase_timestamp),
            DAY
        ) AS days_before_purchase,
        p.article_id,
        p.section,
        p.page_level,
        i.user_pseudo_id as user_id
    FROM
        purchase_ids i
        LEFT JOIN pages p ON i.user_pseudo_id = p.user_pseudo_id
    WHERE
        p.article_id IS NOT NULL
        AND p.page_view_timestamp < i.purchase_timestamp
        AND TIMESTAMP_MICROS (p.page_view_timestamp) >= TIMESTAMP_SUB (
            TIMESTAMP_MICROS (i.purchase_timestamp),
            INTERVAL 30 DAY
        )
)
    -- Select the final output 
    SELECT r.purchase_date, r.days_before_purchase, r.article_id, r.section, r.page_level, r.user_id FROM raw r); END IF; 