-- Get top 5 recommended products per user based on view history
WITH ranked_products AS (
    SELECT 
        user_id,
        product_id,
        COUNT(*) AS views,
        RANK() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rank
    FROM events
    WHERE event_type = 'view'
    GROUP BY user_id, product_id
)
SELECT user_id, product_id, views
FROM ranked_products
WHERE rank <= 5;
