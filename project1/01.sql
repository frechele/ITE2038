SELECT name FROM Trainer, (SELECT owner_id FROM CatchedPokemon
	GROUP BY owner_id
	HAVING COUNT(*) >= 3) AS owner_id
WHERE id = owner_id;
