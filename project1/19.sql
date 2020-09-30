SELECT COUNT(DISTINCT type) FROM Trainer, Gym, Pokemon AS P, CatchedPokemon AS CP
WHERE leader_id = owner_id AND owner_id = Trainer.id AND hometown = 'Sangnok City' AND P.id = pid;
