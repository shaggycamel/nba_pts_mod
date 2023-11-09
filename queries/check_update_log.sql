WITH cte_max_date AS (SELECT MAX(process_date::DATE) FROM util.update_log)

SELECT * 
FROM util.update_log 
WHERE table_name = 'nba.player_game_log'
  AND successful_run IS FALSE
  AND process_date::DATE = (SELECT * FROM cte_max_date)