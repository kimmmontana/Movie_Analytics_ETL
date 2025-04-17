CREATE OR REPLACE VIEW genre_by_profit AS
WITH genre_profit AS (
    SELECT
        g.genre,
        SUM(profit) AS total_profit,
        AVG(profit) AS avg_profit,
        COUNT(DISTINCT f.id) AS movie_count
    FROM fact_movies f
    JOIN dim_movies_genres g ON f.id = g.id
    GROUP BY g.genre
),
top_genre_by_profit AS (
    SELECT *
    FROM genre_profit
    ORDER BY total_profit DESC
)

SELECT *
FROM top_genre_by_profit;
