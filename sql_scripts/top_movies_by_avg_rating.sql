CREATE VIEW top_movies_by_avg_rating AS
WITH top_movies_by_avg_rating AS (
    SELECT 
        fm.id,
        dm.title,
        fm.avg_rating
    FROM 
        fact_movies fm
    LEFT JOIN 
        dim_movies dm ON fm.id = dm.id
    WHERE 
        fm.avg_rating > 0
    ORDER BY 
        fm.avg_rating DESC
)
SELECT 
    id,
    title,
    avg_rating
FROM 
    top_movies_by_avg_rating;
