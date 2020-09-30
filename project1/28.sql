SELECT Trainer.name, AVG(level) AS avg_level FROM CatchedPokemon
JOIN Pokemon ON Pokemon.id = pid
JOIN Trainer ON Trainer.id = owner_id
WHERE type = 'Normal' OR type = 'Electric'
GROUP BY owner_id
ORDER BY avg_level ASC;
