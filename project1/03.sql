SELECT AVG(level) FROM Pokemon, (SELECT pid, level FROM Trainer AS T, CatchedPokemon AS C
	WHERE T.id = C.owner_id AND T.hometown = 'Sangnok City') AS pid
WHERE id = pid AND type = 'Electric';
