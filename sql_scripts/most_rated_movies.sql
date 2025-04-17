CREATE VIEW most_rated_movies AS 
WITH most_rated_movies AS (
    SELECT 
        fm.id,
        dm.title,
        fm.Total_ratings
    FROM 
        fact_movies fm
    LEFT JOIN 
        dim_movies dm ON fm.id = dm.id
    ORDER BY 
        fm.Total_ratings DESC
)
SELECT 
    id,
    title,
    Total_ratings
FROM 
    most_rated_movies;
