SELECT * 
FROM util.update_log 
WHERE table_name = 'nba.player_game_log'
  AND successful_run IS FALSE
  AND process_date::DATE = '{cur_date}'