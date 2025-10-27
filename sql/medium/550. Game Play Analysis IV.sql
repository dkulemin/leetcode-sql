/*
Table: Activity

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| player_id    | int     |
| device_id    | int     |
| event_date   | date    |
| games_played | int     |
+--------------+---------+
(player_id, event_date) is the primary key (combination of columns with unique values) of this table.
This table shows the activity of players of some games.
Each row is a record of a player who logged in and played a number of games (possibly 0) before logging out on someday using some device.

Write a solution to report the fraction of players that logged in again on the day after the day they first logged in, rounded to 2 decimal places.
In other words, you need to determine the number of players who logged in on the day immediately following their initial login, and divide it by the number of total players.
https://leetcode.com/problems/game-play-analysis-iv/description/
*/

WITH ranked_activity AS (
    SELECT
        player_id,
        event_date,
        FIRST_VALUE(event_date) OVER (PARTITION BY player_id ORDER BY event_date) first_event_date,
        DENSE_RANK() OVER (ORDER BY player_id) player_id_rnk
    FROM player_activity
)
SELECT 
    ROUND(COUNT(
        CASE 
            WHEN first_event_date = event_date - INTERVAL '1 DAY' THEN 1
            ELSE NULL
        END
    ) / MAX(player_id_rnk)::NUMERIC, 2) fraction
FROM ranked_activity;
