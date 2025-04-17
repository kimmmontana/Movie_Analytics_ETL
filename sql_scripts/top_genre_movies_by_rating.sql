CREATE VIEW top_genre_movies_by_rating AS
WITH top_genre_movies_by_rating AS (
    SELECT distinct
        dmg.genre,
        fm.id,
        dm.title,
        fm.avg_rating
    FROM 
        fact_movies fm
    JOIN 
        dim_movies dm ON fm.id = dm.id
    JOIN 
        dim_movies_genres dmg ON dm.id = dmg.id
    WHERE 
        fm.avg_rating > 0
    ORDER BY 
        fm.avg_rating DESC
)
SELECT 
    genre,
    id,
    title,
    avg_rating
FROM 
    top_genre_movies_by_rating;
