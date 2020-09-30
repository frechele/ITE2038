SELECT name FROM Pokemon, Evolution
WHERE id = before_id AND before_id > after_id
ORDER BY name ASC;
