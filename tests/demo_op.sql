SELECT count(*) FROM bench;
SELECT id FROM bench WHERE id >= 1 LIMIT 1;
SELECT name FROM bench WHERE id = 199990;
SELECT id,name FROM bench WHERE id >= 199000 LIMIT 10;
SELECT COUNT(*) AS c FROM bench WHERE id >= 100000;
SELECT COUNT(*) AS c FROM bench WHERE grade = 'A';
SELECT id,name,age FROM bench WHERE id >= 199000 ORDER BY id ASC LIMIT 5;
SELECT id,name FROM bench WHERE id >= 199000 LIMIT 100 OFFSET 100;
SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age FROM bench GROUP BY grade ORDER BY cnt DESC;

\drop_index bench idx_id
\drop_index bench idx_grade
SELECT id FROM bench WHERE id >= 1 LIMIT 1;
SELECT name FROM bench WHERE id = 199990;
SELECT id,name FROM bench WHERE id >= 199000 LIMIT 10;
SELECT COUNT(*) AS c FROM bench WHERE id >= 100000;
SELECT COUNT(*) AS c FROM bench WHERE grade = 'A';
SELECT id,name,age FROM bench WHERE id >= 199000 ORDER BY id ASC LIMIT 5;
SELECT id,name FROM bench WHERE id >= 199000 LIMIT 100 OFFSET 100;
SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age FROM bench GROUP BY grade ORDER BY cnt DESC;
