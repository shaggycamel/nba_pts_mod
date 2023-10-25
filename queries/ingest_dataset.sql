WITH cte_schedule AS (
    SELECT game_id, LEFT(slug_matchup, 3) AS team, game_date FROM nba.league_game_schedule WHERE game_date >= '2023-10-19'
    UNION
    SELECT game_id, RIGHT(slug_matchup, 3)AS team, game_date FROM nba.league_game_schedule WHERE game_date >= '2023-10-19'
)

SELECT team, game_date, game_id, LEAD(game_id) OVER (PARTITION BY team ORDER BY game_date) AS next_game_id
FROM cte_schedule 