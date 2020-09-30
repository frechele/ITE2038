SELECT City.name, AVG(level) AS avg FROM CatchedPokemon
JOIN Trainer ON Trainer.id = owner_id
RIGHT OUTER JOIN City ON City.name = hometown
GROUP BY City.name
ORDER BY avg ASC;