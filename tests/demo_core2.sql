
SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age
FROM student
GROUP BY grade
ORDER BY grade ASC;

DELETE FROM student WHERE grade = 'C';

SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age
FROM student
GROUP BY avg_age
ORDER BY grade ASC;


SELECT s.grade, COUNT(*) AS student_count
FROM student s INNER JOIN course c ON s.id = c.course_id WHERE s.age > 18
GROUP BY s.grade HAVING COUNT(*) > 0
ORDER BY student_count DESC LIMIT 10;

SELECT s.name, c.course_name
FROM student s
INNER JOIN course c ON s.id = c.course_id
ORDER BY s.name;


\popup
\export D:\360MoveData\Users\GQL19\Desktop\SC.xlsx


