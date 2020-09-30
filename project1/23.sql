SELECT DISTINCT name FROM Trainer AS T, CatchedPokemon AS CP
WHERE T.id = CP.owner_id AND level <= 10
ORDER BY name ASC;
