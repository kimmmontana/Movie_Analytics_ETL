CREATE VIEW top_movies_by_profit AS
WITH top_movies_by_profit AS (
    SELECT 
        fm.id,
        dm.title,
        fm.release_date,
        fm.budget,
        fm.revenue,
        (fm.revenue - fm.budget) AS profit,
        fm.avg_rating,
        fm.total_ratings,
        fm.std_dev,
        dm.last_rated
    FROM 
        fact_movies fm
    JOIN 
        dim_movies dm ON fm.id = dm.id
    WHERE 
        fm.revenue > 0 AND fm.budget > 0  -- Ensure valid profit
)
SELECT 
    title,
    release_date,
    profit,
    revenue,
    budget,
    avg_rating,
    total_ratings
FROM 
    top_movies_by_profit
ORDER BY 
    profit DESC  -- Sorting by profit in descending order to show the top movies  -- Showing top 10 movies with the highest profit
