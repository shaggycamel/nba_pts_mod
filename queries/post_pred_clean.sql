SELECT player_id, player, team_slug 
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY slug_season, player_id ORDER BY how_acquired DESC) AS rn FROM nba.team_roster) AS inner_q
WHERE inner_q.rn = 1
  AND slug_season = '2023-24'