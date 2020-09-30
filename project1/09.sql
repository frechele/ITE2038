SELECT GL.name, AVG(level) FROM CatchedPokemon, (SELECT id, name FROM Trainer, Gym WHERE id = leader_id) AS GL
WHERE owner_id = GL.id
GROUP BY GL.id
ORDER BY GL.name ASC;
