SELECT Trainer.id, COUNT(*) AS cnt FROM CatchedPokemon
JOIN Trainer on Trainer.id = owner_id
GROUP BY owner_id
HAVING cnt = (SELECT MAX(x.cnt) FROM (SELECT COUNT(*) AS cnt FROM CatchedPokemon GROUP BY owner_id) x)
ORDER BY Trainer.id ASC;
