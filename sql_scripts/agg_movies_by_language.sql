CREATE VIEW agg_movies_by_language AS 
WITH agg_movies_by_language AS (
    SELECT 
        dm.title,
        fm.avg_rating,
        dl.language_iso,
        dl.language
    FROM 
        fact_movies fm
    LEFT JOIN 
        dim_movies dm ON fm.id = dm.id
    LEFT JOIN 
        dim_movies_languages dl ON dm.id = dl.id
    WHERE 
        fm.avg_rating > 0
)
SELECT 
    language,
    COUNT(*) AS num_movies,
    AVG(avg_rating) AS avg_rating
FROM 
    agg_movies_by_language
GROUP BY 
    language
ORDER BY 
    avg_rating DESC;
