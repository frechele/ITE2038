SELECT AVG(level) FROM Trainer, CatchedPokemon
WHERE Trainer.id = owner_id AND name = 'Red';
