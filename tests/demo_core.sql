CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);
INSERT INTO student (id,name,age,grade) VALUES
(1,'Alice',20,'A'),
(2,'Bob',19,'B'),
(3,'Carol',21,'A'),
(4,'Dave',22,'B'),
(5,'Eve',23,'C'),
(6,'Frank',20,'A'),
(7,'Grace',21,'B'),
(8,'Heidi',22,'C'),
(9,'Ivan',19,'A'),
(10,'Judy',20,'B');

SELECT id,name,age,grade FROM student ORDER BY id ASC;

SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age
FROM student
GROUP BY grade
ORDER BY grade ASC;

UPDATE student SET age=22, grade='A+' WHERE id=3;
SELECT id,name,age,grade FROM student WHERE id=3;
\create_index student id idx_id
SELECT id,name FROM student WHERE id = 5;
DELETE FROM student WHERE grade = 'C';
SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age
FROM student
GROUP BY grade
ORDER BY grade ASC;
