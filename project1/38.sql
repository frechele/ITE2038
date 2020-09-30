SELECT name FROM Evolution
JOIN Pokemon ON id = after_id
WHERE after_id NOT IN (SELECT before_id FROM Evolution)
ORDER BY name ASC;
