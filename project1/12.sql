SELECT DISTINCT name, type FROM CatchedPokemon AS C, Pokemon AS P
WHERE C.pid = P.id AND level >= 30
ORDER BY name ASC;
