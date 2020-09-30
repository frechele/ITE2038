SELECT City.name, nickname FROM Pokemon AS P
JOIN CatchedPokemon AS CP ON P.id = pid
JOIN Trainer AS T ON T.id = owner_id
JOIN (SELECT hometown, MAX(level) AS m FROM CatchedPokemon
     JOIN Trainer ON Trainer.id = owner_id
     GROUP BY hometown) mtbl ON T.hometown = mtbl.hometown AND level = mtbl.m
RIGHT OUTER JOIN City ON City.name = T.hometown
ORDER BY City.name ASC;
