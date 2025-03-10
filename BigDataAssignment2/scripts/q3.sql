-- Enable full-text search on category_code for product search
SELECT DISTINCT product_id, category_code, brand, price
FROM events
WHERE to_tsvector(category_code) @@ plainto_tsquery('electronics OR smartphone OR laptop');
