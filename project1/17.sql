SELECT COUNT(*) FROM (SELECT DISTINCT pid FROM Trainer AS T, CatchedPokemon AS CP
		WHERE T.id = CP.owner_id AND T.hometown = 'Sangnok City') AS Types;
