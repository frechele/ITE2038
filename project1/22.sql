SELECT type, COUNT(*) AS cnt FROM Pokemon
GROUP BY type
ORDER BY cnt ASC, type ASC;
