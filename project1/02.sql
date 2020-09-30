SELECT name FROM Pokemon
WHERE type IN 
	(SELECT type FROM Pokemon GROUP BY type HAVING COUNT(*) >= 
     (SELECT MAX(cnt) FROM (SELECT COUNT(*) AS cnt FROM Pokemon GROUP BY type HAVING cnt < (SELECT MAX(cnt) FROM (SELECT COUNT(*) AS cnt FROM Pokemon GROUP BY type) x)) x))
ORDER BY name ASC;
