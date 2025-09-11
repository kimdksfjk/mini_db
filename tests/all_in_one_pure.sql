CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);
INSERT INTO student (id,name,age,grade) VALUES (1,'Alice',20,'A');
INSERT INTO student (id,name,age,grade) VALUES (2,'Bob',19,'B'),(3,'Carol',21,'A');
SELECT * FROM student;
SELECT id,name,age FROM student WHERE age > 19 ORDER BY age DESC;
SELECT name,grade FROM student;
SELECT id,name,age FROM student ORDER BY id ASC LIMIT 2 OFFSET 1;
INSERT INTO student (id,name,age,grade) VALUES (4,'Dave','22','B');
SELECT id,name,age FROM student WHERE age >= 21 ORDER BY id;
INSERT INTO student (id,name,age,grade) VALUES
(5,'U1',18,'C'),(6,'U2',19,'B'),(7,'U3',20,'B'),(8,'U4',21,'A'),(9,'U5',22,'A'),
(10,'U6',23,'B'),(11,'U7',18,'C'),(12,'U8',19,'B'),(13,'U9',20,'B'),(14,'U10',21,'A'),
(15,'U11',22,'A'),(16,'U12',23,'B'),(17,'U13',18,'C'),(18,'U14',19,'B'),(19,'U15',20,'B'),
(20,'U16',21,'A'),(21,'U17',22,'A'),(22,'U18',23,'B'),(23,'U19',24,'A'),(24,'U20',25,'A');
SELECT id,age FROM student ORDER BY id ASC LIMIT 10 OFFSET 0;
DELETE FROM student;
SELECT * FROM student;
CREATE TABLE course(cid INT, title VARCHAR);
INSERT INTO course (cid,title) VALUES (101,'DB'),(102,'OS');
SELECT * FROM course ORDER BY cid ASC;
