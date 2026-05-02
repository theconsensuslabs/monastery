DROP TABLE IF EXISTS shoes;
CREATE TABLE shoes (left_shoe TEXT, right_shoe TEXT, shoe_id INT PRIMARY KEY);
INSERT INTO shoes VALUES ('', '', 1);

---

t1: BEGIN;
t2: BEGIN;
t1: UPDATE shoes SET left_shoe = 'Lin' WHERE shoe_id = 1;
t2: UPDATE shoes SET left_shoe = 'Carlos' WHERE shoe_id = 1;
t2: UPDATE shoes SET right_shoe = 'Carlos' WHERE shoe_id = 1;
t1: UPDATE shoes SET right_shoe = 'Lin' WHERE shoe_id = 1;
t1: SELECT * FROM shoes; # Inconsistent results here would mean dirty reads not necessarily dirty writes.
t2: SELECT * FROM shoes; # Inconsistent results here would mean dirty reads not necessarily dirty writes.
t1: COMMIT;
t2: COMMIT;
t1: SELECT * FROM shoes; -- assert ({Lin, Lin, 1}) or ({Carlos, Carlos, 1})
t2: SELECT * FROM shoes; -- assert ({Lin, Lin, 1}) or ({Carlos, Carlos, 1})
