WITH mapped_sessions AS (
    SELECT
        id_user,
        action,
        date_action,
        LEAD(date_action) OVER (PARTITION BY id_user ORDER BY date_action) AS next_action_time,
        LEAD(action) OVER (PARTITION BY id_user ORDER BY date_action) AS next_action_type
    FROM platform.users_sessions
    -- filtering for 10 days from now
    WHERE DATE(date_action) BETWEEN CURRENT_DATE - INTERVAL '9 days' AND CURRENT_DATE
),
paired_sessions AS (
    SELECT
        id_user,
        date_action AS open_time,
        CASE
            WHEN next_action_type = 'close' THEN next_action_time
            -- assuming that not closed sessions we will count from session start to now
            -- alternatively we can mark such sessions as not closed and skip them
            ELSE NOW()
        END AS close_time
    FROM mapped_sessions
    WHERE action = 'open'
),
session_durations AS (
    SELECT
        id_user,
        DATE(open_time) AS session_date,
        EXTRACT(EPOCH FROM (close_time - open_time))/3600.0 AS hours_spent
    FROM paired_sessions
)
SELECT
    id_user,
    session_date,
    SUM(hours_spent) AS total_hours
FROM session_durations
GROUP BY id_user, session_date
ORDER BY id_user, session_date;
