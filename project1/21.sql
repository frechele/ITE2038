SELECT T.name, COUNT(*) FROM Trainer AS T, CatchedPokemon AS CP, Gym
WHERE T.id = leader_id AND T.id = CP.owner_id
GROUP BY T.id
ORDER BY T.name ASC;
