CREATE OR REPLACE VIEW movies_by_genre_country_year AS
WITH movie_base AS (
    SELECT
        f.id,
        YEAR(f.release_date) AS release_year,
        g.genre,
        c.country
    FROM fact_movies f
    JOIN dim_movies_genres g ON f.id = g.id
    JOIN dim_movies_countries c ON f.id = c.id
    WHERE f.release_date IS NOT NULL
),
movies_by_group AS (
    SELECT
        release_year,
        genre,
        country,
        COUNT(DISTINCT id) AS total_movies
    FROM movie_base
    GROUP BY release_year, genre, country
)

SELECT *
FROM movies_by_group
ORDER BY release_year, genre, country;
