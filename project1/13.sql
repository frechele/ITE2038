SELECT P.name, P.id FROM Trainer AS T, CatchedPokemon AS CP, Pokemon AS P
WHERE T.id = CP.owner_id AND CP.pid = P.id AND T.hometown = 'Sangnok City'
ORDER BY P.id ASC;
