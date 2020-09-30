SELECT name FROM Trainer AS T, CatchedPokemon AS CP
WHERE T.id = owner_id AND pid NOT IN (SELECT before_id FROM Evolution) AND pid IN (SELECT after_id FROM Evolution)
ORDER BY name ASC;
