SELECT COUNT(*) FROM Pokemon AS P, CatchedPokemon AS CP
WHERE CP.pid = P.id
GROUP BY type
ORDER BY type ASC;
