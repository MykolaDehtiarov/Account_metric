# Account_metric
#Необхідно зібрати дані, які допоможуть аналізувати динаміку створення акаунтів, активність користувачів за листами (відправлення, відкриття, переходи).
#A також оцінювати поведінку в категоріях, таких як інтервал відправлення, верифікація акаунтів і статус підписки.
#Дані дозволять порівнювати активність між країнами, визначати ключові ринки, сегментувати користувачів за різними параметрами.

WITH account_metrics AS (
    SELECT
        CAST(s.date AS DATE) AS date,
        sp.country AS country,
        a.send_interval AS send_interval,
        a.is_verified AS is_verified,
        a.is_unsubscribed AS is_unsubscribed,
        COUNT(a.id) AS account_cnt,
        0 AS sent_msg,
        0 AS open_msg,
        0 AS visit_msg
    FROM
        `data-analytics-mate.DA.account` AS a
    JOIN
        `data-analytics-mate.DA.account_session` AS acs
        ON a.id = acs.account_id
    JOIN
        `data-analytics-mate.DA.session` AS s
        ON acs.ga_session_id = s.ga_session_id
    JOIN
        `data-analytics-mate.DA.session_params` AS sp
        ON s.ga_session_id = sp.ga_session_id
    GROUP BY
        1, 2, 3, 4, 5
),
email_metrics AS (
    SELECT
        -- Corrected logic: Add the 'sent_date' (which is an INT64 representing days offset)
        -- to the session date associated with the account.
        -- We need to ensure 's.date' is accessible here.
        DATE_ADD(CAST(s.date AS DATE), INTERVAL CAST(es.sent_date AS INT64) DAY) AS date,
        sp.country AS country,
        a.send_interval AS send_interval,
        a.is_verified AS is_verified,
        a.is_unsubscribed AS is_unsubscribed,
        0 AS account_cnt,
        COUNT(DISTINCT es.id_message) AS sent_msg,
        COUNT(DISTINCT eo.id_message) AS open_msg,
        COUNT(DISTINCT ev.id_message) AS visit_msg
    FROM
        `data-analytics-mate.DA.email_sent` AS es
    LEFT JOIN
        `data-analytics-mate.DA.email_open` AS eo
        ON es.id_message = eo.id_message
    LEFT JOIN
        `data-analytics-mate.DA.email_visit` AS ev
        ON es.id_message = ev.id_message
    LEFT JOIN
        `data-analytics-mate.DA.account` AS a
        ON es.id_account = a.id
    LEFT JOIN
        `data-analytics-mate.DA.account_session` AS acs
        ON a.id = acs.account_id
    LEFT JOIN
        `data-analytics-mate.DA.session` AS s
        ON acs.ga_session_id = s.ga_session_id
    LEFT JOIN
        `data-analytics-mate.DA.session_params` AS sp
        ON s.ga_session_id = sp.ga_session_id
    GROUP BY
        1, 2, 3, 4, 5
),
combined_metrics AS (
    SELECT * FROM account_metrics
    UNION ALL
    SELECT * FROM email_metrics
),
daily_grouped_metrics AS (
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM(account_cnt) AS account_cnt,
        SUM(sent_msg) AS sent_msg,
        SUM(open_msg) AS open_msg,
        SUM(visit_msg) AS visit_msg
    FROM
        combined_metrics
    GROUP BY
        1, 2, 3, 4, 5
),
country_totals AS (
    SELECT
        country,
        SUM(account_cnt) AS total_country_account_cnt,
        SUM(sent_msg) AS total_country_sent_cnt
    FROM
        daily_grouped_metrics
    GROUP BY
        country
),
ranked_metrics AS (
    SELECT
        dgm.date,
        dgm.country,
        dgm.send_interval,
        dgm.is_verified,
        dgm.is_unsubscribed,
        dgm.account_cnt,
        dgm.sent_msg,
        dgm.open_msg,
        dgm.visit_msg,
        ct.total_country_account_cnt,
        ct.total_country_sent_cnt,
        DENSE_RANK() OVER (ORDER BY ct.total_country_account_cnt DESC) AS rank_total_country_account_cnt,
        DENSE_RANK() OVER (ORDER BY ct.total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
    FROM
        daily_grouped_metrics AS dgm
    JOIN
        country_totals AS ct
        ON dgm.country = ct.country
)
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    total_country_account_cnt,
    total_country_sent_cnt,
    rank_total_country_account_cnt,
    rank_total_country_sent_cnt
FROM
    ranked_metrics
WHERE
    rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10
ORDER BY
    date, country, send_interval, is_verified, is_unsubscribed;
