SELECT name FROM CatchedPokemon
JOIN Trainer ON Trainer.id = owner_id
GROUP BY owner_id, pid
HAVING COUNT(*) >= 2
ORDER BY name ASC;
