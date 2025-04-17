CREATE VIEW agg_movies_by_country AS
WITH agg_movies_by_country AS (
    SELECT 
        dm.title,
        fm.revenue,
        fm.budget,
        fm.profit,
        dc.country
    FROM 
        fact_movies fm
    JOIN 
        dim_movies dm ON fm.id = dm.id
    JOIN 
        dim_movies_countries dc ON dm.id = dc.id
    WHERE 
        fm.revenue > 0 AND fm.budget > 0
)
SELECT 
    country,
    COUNT(*) AS num_movies,
    SUM(revenue) AS total_revenue,
    SUM(budget) AS total_budget,
    SUM(profit) AS total_profit
FROM 
    agg_movies_by_country
GROUP BY 
    country
ORDER BY 
    total_profit DESC;
