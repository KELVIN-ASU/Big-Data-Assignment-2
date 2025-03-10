-- Find campaign effectiveness by tracking purchases
SELECT 
    c.id AS campaign_id,
    c.topic AS campaign_topic,
    COUNT(DISTINCT m.client_id) AS total_recipients,
    COUNT(DISTINCT e.user_id) AS total_purchasers,
    ROUND((COUNT(DISTINCT e.user_id)::DECIMAL / COUNT(DISTINCT m.client_id)) * 100, 2) AS conversion_rate
FROM messages m
JOIN campaigns c ON m.campaign_id = c.id
LEFT JOIN events e ON m.client_id = e.user_id AND e.event_type = 'purchase'
GROUP BY c.id, c.topic
ORDER BY conversion_rate DESC;
