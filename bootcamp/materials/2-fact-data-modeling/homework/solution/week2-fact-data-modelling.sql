--Query to deduplicate game_details
WITH deduped_gds AS (
    SELECT
        gds.*,
        ROW_NUMBER() OVER(PARTITION BY gds.game_id, gds.team_id, gds.player_id ORDER BY gds.game_id) as row_num
    FROM game_details gds
   )
   
SELECT * FROM deduped_gds
WHERE row_num = 1;

-- A DDL for a user_devices_cumulated
CREATE TABLE user_devices_cumulated (
user_id TEXT,
device_id NUMERIC,
browser_type TEXT,
device_activity_datelist DATE[],
date DATE,
PRIMARY KEY (user_id, device_id, browser_type, date)
);

--A cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-05')
),
    today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        device_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-06')
        AND user_id IS NOT NULL
        GROUP BY user_id, device_id, DATE(CAST(event_time AS TIMESTAMP))
    ),
deduplicated AS
    (SELECT COALESCE(t.user_id, y.user_id) AS user_id,
                    t.device_id,
                    d.browser_type,
                    CASE
                        WHEN y.device_activity_datelist IS NULL
                            THEN ARRAY [t.date_active]
                        WHEN t.date_active IS NULL THEN y.device_activity_datelist
                        ELSE ARRAY [t.date_active] || y.device_activity_datelist
                    END AS device_activity_datelist,
                    COALESCE(t.date_active, y.date + Interval '1 day') AS date,
                    ROW_NUMBER() OVER (PARTITION BY COALESCE(t.user_id, y.user_id),
                        t.device_id, d.browser_type,
                        COALESCE(t.date_active, y.date + INTERVAL '1 day') ORDER BY t.date_active) AS row_num
                 FROM today t
                          FULL OUTER JOIN yesterday y
                                          ON t.user_id = y.user_id
                          JOIN devices d
                               ON t.device_id = d.device_id
                )
SELECT user_id, device_id, browser_type, device_activity_datelist, date
FROM deduplicated
WHERE row_num = 1
ON CONFLICT (user_id, device_id, browser_type, date)
DO UPDATE SET
    device_activity_datelist = EXCLUDED.device_activity_datelist;

-- 1st - create a new column in table 'user_devices_cumulated'
ALTER TABLE user_devices_cumulated
ADD COLUMN datelist_int BIGINT[];

-- 2nd - populate column with the integer representation of the device_activity_datelist
UPDATE user_devices_cumulated
SET datelist_int = ARRAY(
    SELECT EXTRACT(EPOCH FROM unnest(device_activity_datelist))::BIGINT
);

-- A DDL for a host_cumulated
CREATE TABLE hosts_cumulated (
host TEXT,
host_activity_datelist DATE[],
date DATE,
PRIMARY KEY (host, date)
);

-- The incremental query to generate host_activity_datelist
INSERT INTO hosts_cumulated
WITH activity_dates AS (
    SELECT
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS activity_date -- Convert event_time to date
    FROM events
    WHERE event_time IS NOT NULL
)
SELECT
    host,
    ARRAY_AGG(DISTINCT activity_date ORDER BY activity_date) AS host_activity_datelist,
    MAX(activity_date) AS date
FROM activity_dates
GROUP BY host;

--A monthly, reduced fact table DDL host_activity_reduced
CREATE TABLE host_activity_reduced (
     month DATE,                     -- The month for aggregation
     host TEXT,                      -- The host identifier
     hit_array INT,                  -- Count of total hits
     unique_visitors TEXT[],      -- Count of unique users
    PRIMARY KEY (month, host)       -- Ensure uniqueness of each host per month
);

--An incremental query that loads host_activity_reduced
INSERT INTO host_activity_reduced
WITH activity_data AS (
    SELECT
        DATE_TRUNC('month', CAST(event_time AS TIMESTAMP)) AS month,  -- month from event_time
        host,
        user_id
    FROM events
    WHERE event_time IS NOT NULL
),
host_months AS (
    -- Generate a list of all possible host-month combinations
    SELECT DISTINCT
        DATE_TRUNC('month', CAST(event_time AS TIMESTAMP)) AS month,
        host
    FROM events
    WHERE event_time IS NOT NULL
),
aggregated_data AS (
    SELECT
        hm.month,
        hm.host,
        COUNT(ad.user_id) AS hit_array,  -- Count total events for the month-host
        ARRAY_AGG(DISTINCT ad.user_id) AS unique_visitors -- Unique visitors for the month-host
    FROM host_months hm
    LEFT JOIN activity_data ad
        ON hm.month = ad.month AND hm.host = ad.host
    GROUP BY hm.month, hm.host
)
SELECT
    month,
    host,
    COALESCE(hit_array, 0) AS hit_array,  -- Default to 0 hits for missing data
    COALESCE(unique_visitors, ARRAY[]::BIGINT[]) AS unique_visitors -- Default to empty array for visitors
FROM aggregated_data;

CREATE TABLE user_devices_cumulated (
user_id NUMERIC,
device_id NUMERIC,
browser_type TEXT,
device_activity_datelist DATE[],
PRIMARY KEY (user_id, device_id, browser_type)
);
