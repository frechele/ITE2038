SELECT DISTINCT name
FROM (SELECT CP.pid FROM Trainer AS T, CatchedPokemon AS CP WHERE T.id = CP.owner_id AND hometown = 'Brown City') brown, Pokemon
WHERE pid IN (SELECT CP.pid FROM Trainer AS T, CatchedPokemon AS CP WHERE T.id = CP.owner_id AND hometown = 'Sangnok City') AND pid = Pokemon.id
ORDER BY name ASC;
