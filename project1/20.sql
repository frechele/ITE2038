SELECT T.name, COUNT(*) AS cnt FROM Trainer AS T, CatchedPokemon AS CP
WHERE T.id = CP.owner_id AND hometown = 'Sangnok City'
GROUP BY T.id
ORDER BY cnt ASC;
