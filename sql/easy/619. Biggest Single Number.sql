/*
Table: MyNumbers

+-------------+------+
| Column Name | Type |
+-------------+------+
| num         | int  |
+-------------+------+
This table may contain duplicates (In other words, there is no primary key for this table in SQL).
Each row of this table contains an integer.
 

A single number is a number that appeared only once in the MyNumbers table.

Find the largest single number. If there is no single number, report null.

https://leetcode.com/problems/biggest-single-number/description/
*/

SELECT MAX(num) num
FROM (
    SELECT num, COUNT(num)
    FROM mynumbers
    GROUP BY num
    HAVING COUNT(num) = 1
) t;
