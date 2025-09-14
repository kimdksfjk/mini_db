SELECT id,name,age,grade FROM student ORDER BY id ASC;

SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age
FROM student
GROUP BY grade
ORDER BY grade ASC;

\popup
\export agg.xlsx
