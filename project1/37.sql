SELECT name, SUM(level) AS s FROM CatchedPokemon
JOIN Trainer on Trainer.id = owner_id
GROUP BY owner_id
HAVING s = (SELECT MAX(x.s) FROM (SELECT SUM(level) AS s FROM CatchedPokemon GROUP BY owner_id) x)
ORDER BY name ASC;
