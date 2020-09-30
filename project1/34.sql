SELECT name, level, nickname FROM Gym, Pokemon AS P, CatchedPokemon AS CP
WHERE P.id = CP.pid AND owner_id = leader_id AND nickname LIKE 'A%'
ORDER BY name DESC;
