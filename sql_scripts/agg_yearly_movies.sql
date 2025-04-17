#CREATE OR REPLACE VIEW agg_yearly_movies AS 
WITH agg_yearly_movies AS (
    SELECT 
        dm.title,
        fm.release_date,
        fm.revenue,
        fm.budget,
        (fm.revenue - fm.budget) AS profit,
        EXTRACT(YEAR FROM fm.release_date) AS release_year
    FROM 
        fact_movies fm
    LEFT JOIN 
        dim_movies dm ON fm.id = dm.id
    WHERE 
        fm.revenue > 0 AND fm.budget > 0
)
SELECT 
    release_year,
    COUNT(*) AS num_movies,
    SUM(revenue) AS total_revenue,
    SUM(budget) AS total_budget,
    SUM(profit) AS total_profit
FROM 
    agg_yearly_movies
GROUP BY 
    release_year
ORDER BY 
    release_year DESC;
