SELECT name FROM Pokemon AS P, CatchedPokemon AS CP
WHERE pid = P.id AND nickname LIKE '% %'
ORDER BY name DESC;
