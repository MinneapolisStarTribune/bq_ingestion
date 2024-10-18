IF (
    EXISTS (
        SELECT
            1
        FROM
            `PTC.user_id`
        WHERE
            purchase_date = FORMAT_DATE (
                "%Y-%m-%d",
                DATE_SUB (CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            )
    )
)
AND (
    NOT EXISTS (
        SELECT
            1
        FROM
            `PTC.user_id_partitioned`
        WHERE
            CAST(purchase_date AS STRING) = FORMAT_DATE (
                "%Y-%m-%d",
                DATE_SUB (CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            )
    )
)
AND (
    EXISTS (
        SELECT
            creation_time,
            table_name
        FROM
            `daring-phoenix-295916.custom_reporting.INFORMATION_SCHEMA.TABLES`
        WHERE
            table_name = 'ga4_stories_yesterday'
            AND date (creation_time) = CURRENT_DATE('America/Chicago')
    )
) THEN INSERT `PTC.user_id_partitioned`
select
    DATE (p.purchase_date) as purchase_date,
    p.days_before_purchase,
    p.article_id,
    p.section,
    p.page_level,
    s.author,
    s.subsection,
    s.page_name,
    p.user_id,
    PARSE_DATE ("%B %d, %Y", INITCAP (s.publish_date)) as pub_date
from
    `PTC.user_id` p
    left join `custom_reporting.ga4_stories_ytd` s on p.article_id = CAST(s.article_id as string) -- where p.purchase_date = FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)) and s.page_name is not null and not contains_substr(publish_date, ":") and publish_date != "invalid date" group by 1,2,3,4,5,6,7,8,9,10; END IF;