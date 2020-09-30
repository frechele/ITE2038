SELECT p1.id, p1.name, p2.name, p3.name FROM Pokemon AS p1, Pokemon AS p2, Pokemon AS p3, Evolution AS e1, Evolution AS e2
WHERE p1.id = e1.before_id AND p2.id = e1.after_id AND p2.id = e2.before_id AND p3.id = e2.after_id
AND p1.id NOT IN (SELECT after_id FROM Evolution)
AND p3.id NOT IN (SELECT before_id FROM Evolution);