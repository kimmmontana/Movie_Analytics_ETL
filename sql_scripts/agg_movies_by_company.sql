CREATE VIEW agg_movies_by_company AS
WITH agg_movies_by_company AS (
    SELECT 
        dm.title,
        fm.revenue,
        fm.budget,
        fm.profit,
        dc.company
    FROM 
        fact_movies fm
    JOIN 
        dim_movies dm ON fm.id = dm.id
    JOIN 
        dim_movies_companies dc ON dm.id = dc.id
    WHERE 
        fm.revenue > 0 AND fm.budget > 0
)
SELECT 
    company,
    COUNT(*) AS num_movies,
    SUM(revenue) AS total_revenue,
    SUM(budget) AS total_budget,
    SUM(profit) AS total_profit
FROM 
    agg_movies_by_company
GROUP BY 
    company
ORDER BY 
    total_profit DESC;
