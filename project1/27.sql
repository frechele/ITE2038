SELECT name, MAX(level) FROM Trainer AS T, CatchedPokemon AS CP
WHERE T.id = owner_id
GROUP BY T.id
HAVING COUNT(*) >= 4
ORDER BY name ASC;
