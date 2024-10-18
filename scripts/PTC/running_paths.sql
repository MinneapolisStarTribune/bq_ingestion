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
)
AND (
    NOT EXISTS (
        SELECT
            DATE (TIMESTAMP_MILLIS (last_modified_time))
        FROM
            `daring-phoenix-295916.PTC.__TABLES__`
        where
            table_id = 'running_paths_sessions'
            AND DATE (TIMESTAMP_MILLIS (last_modified_time)) = CURRENT_DATE('America/Chicago')
    )
) THEN MERGE INTO `PTC.running_paths_sessions` AS target USING (
    SELECT
        COALESCE(CAST(m.article_id AS STRING), p.article_id) as article_id,
        m.page_name,
        m.author,
        PARSE_DATE ("%B %d, %Y", INITCAP (m.publish_date)) as pub_date,
        m.page_section,
        m.subsection,
        coalesce(m.sessions, 0) as sessions,
        coalesce(count(distinct p.user_id), 0) as paths
    FROM
        `custom_reporting.ga4_stories_yesterday` m
        FULL OUTER JOIN (
            SELECT
                *
            FROM
                `PTC.user_id`
            WHERE
                purchase_date = FORMAT_DATE (
                    "%Y-%m-%d",
                    DATE_SUB (CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
                )
                AND regexp_contains (article_id, '^[0-9]+$')
        ) AS p ON CAST(m.article_id as STRING) = p.article_id
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7
) AS source ON target.article_id = CAST(source.article_id AS STRING) WHEN MATCHED THEN
UPDATE
SET
    target.sessions = COALESCE(target.sessions, 0) + COALESCE(source.sessions, 0),
    target.paths = COALESCE(target.paths, 0) + COALESCE(source.paths, 0) WHEN NOT MATCHED THEN INSERT (
        article_id,
        page_name,
        author,
        pub_date,
        page_section,
        subsection,
        sessions,
        paths
    )
VALUES
    (
        CAST(source.article_id AS STRING),
        source.page_name,
        source.author,
        source.pub_date,
        source.page_section,
        source.subsection,
        COALESCE(source.sessions, 0),
        COALESCE(source.paths, 0)
    );

END IF;