CREATE OR REPLACE VIEW language_by_profit AS
WITH language_profit AS (
    SELECT
        l.language,
        l.language_iso,
        SUM(profit) AS total_profit,
        AVG(profit) AS avg_profit,
        COUNT(DISTINCT f.id) AS movie_count
    FROM fact_movies f
    JOIN dim_movies_languages l ON f.id = l.id
    GROUP BY l.language, l.language_iso
),
top_language_by_profit AS (
    SELECT *
    FROM language_profit
    ORDER BY total_profit DESC
)

SELECT *
FROM top_language_by_profit;
