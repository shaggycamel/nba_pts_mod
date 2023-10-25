
-- Teams playing tomorrow
WITH cte_teams_playing_tomorrow AS (
    SELECT LEFT(slug_matchup, 3) AS team_slug FROM nba.league_game_schedule WHERE game_date = CURRENT_DATE - 1
    UNION ALL 
    SELECT RIGHT(slug_matchup, 3) AS team_slug FROM nba.league_game_schedule WHERE game_date = CURRENT_DATE - 1
),

-- Players last game date
cte_players_last_game_date AS (
    SELECT log.player_id, roster.team_slug, MAX(game_date) AS prev_game_date
    FROM nba.player_game_log AS log
    LEFT JOIN (SELECT * FROM nba.team_roster WHERE slug_season = '2023-24') AS roster
        ON log.player_id = roster.player_id
    INNER JOIN cte_teams_playing_tomorrow ON roster.team_slug = cte_teams_playing_tomorrow.team_slug
    GROUP BY log.player_id, roster.team_slug
)

-- Main query
SELECT log.*
FROM nba.player_game_log AS log
INNER JOIN cte_players_last_game_date ON log.player_id = cte_players_last_game_date.player_id
    AND log.game_date = cte_players_last_game_date.prev_game_date


