
-- A cte to select the latest team strucutre of players, this approach
-- takes trades into account
WITH cte_team_latest_roster AS (
    SELECT * 
    FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY slug_season, player_id ORDER BY how_acquired DESC) AS rn FROM nba.team_roster) AS inner_q
    WHERE inner_q.rn = 1
),


-- A cte selecting the latest game logs of every player
cte_prior_game_log AS (
    SELECT 
        log.game_date,
        log.game_id,
        log.player_id,
        LEFT(log.matchup, 3) AS team_slug,
        CASE WHEN log.matchup LIKE '%@%' THEN 'away' ELSE 'home' END AS home_away,
        CASE 
            WHEN roster.position = 'F-C' THEN 'C-F'
            WHEN roster.position = 'G-F' THEN 'F-G'
            ELSE roster.position
        END AS position,    
        roster.weight_kg, 
        roster.height_cm, 
        roster.age,
        log.wl,
        COALESCE(log.min, 0) AS min,
        COALESCE(log.fgm, 0) AS fgm,
        COALESCE(log.fga, 0) AS fga,
        COALESCE(log.fg3_m, 0) AS fg3_m,
        COALESCE(log.fg3_a, 0) AS fg3_a,
        COALESCE(log.ftm, 0) AS ftm,
        COALESCE(log.fta, 0) AS fta,
        COALESCE(log.oreb, 0) AS oreb,
        COALESCE(log.dreb, 0) AS dreb,
        COALESCE(log.reb, 0) AS reb,
        COALESCE(log.ast, 0) AS ast,
        COALESCE(log.stl, 0) AS stl,
        COALESCE(log.blk, 0) AS blk,
        COALESCE(log.pf, 0) AS pf,
        COALESCE(log.plus_minus, 0) AS plus_minus,
        COALESCE(log.pts, 0) AS pts
    FROM nba.player_game_log AS log
    LEFT JOIN cte_team_latest_roster AS roster ON log.slug_season = roster.slug_season
        AND log.player_id = roster.player_id
    WHERE log.slug_season >= '2022-23'
),


-- A cte creating a dummy object of teams playing tomorrow, but
-- is in the same structure as cte_prior_game_log
cte_tomorrow_game_log AS (
    SELECT 
        team_tomorrow.game_date,
        team_tomorrow.game_id,
        roster.player_id,
        team_tomorrow.team_slug,
        CASE WHEN LEFT(team_tomorrow.slug_matchup, 3) != team_tomorrow.team_slug THEN 'away' ELSE 'home' END AS home_away,
        CASE 
            WHEN roster.position = 'F-C' THEN 'C-F'
            WHEN roster.position = 'G-F' THEN 'F-G'
            ELSE roster.position
        END AS position,
        roster.weight_kg, 
        roster.height_cm, 
        roster.age,
        NULL AS wl,
        NULL::bigint AS min,
        NULL::bigint AS fgm,
        NULL::bigint AS fga,
        NULL::bigint AS fg3_m,
        NULL::bigint AS fg3_a,
        NULL::bigint AS ftm,
        NULL::bigint AS fta,
        NULL::bigint AS oreb,
        NULL::bigint AS dreb,
        NULL::bigint AS reb,
        NULL::bigint AS ast,
        NULL::bigint AS stl,
        NULL::bigint AS blk,
        NULL::bigint AS pf,
        NULL::bigint AS plus_minus,
        NULL::bigint AS pts
        
    FROM cte_team_latest_roster AS roster
    INNER JOIN (    
        SELECT slug_season, slug_matchup, LEFT(slug_matchup, 3) AS team_slug, game_date, game_id FROM nba.league_game_schedule WHERE game_date = (SELECT MAX(game_date) + 1 FROM cte_prior_game_log)
        UNION ALL
        SELECT slug_season, slug_matchup, RIGHT(slug_matchup, 3) AS team_slug, game_date, game_id FROM nba.league_game_schedule WHERE game_date = (SELECT MAX(game_date) + 1 FROM cte_prior_game_log)
    ) AS team_tomorrow ON roster.slug_season = team_tomorrow.slug_season
        AND roster.team_slug = team_tomorrow.team_slug
),


-- A cte to combining cte_prior_game_log & cte_tomorrow_game_log, and limits players 
-- in cte_prior_game_log to just those playing tomorrow (ie, present in cte_tomorrow_game_log)
cte_pre_rolling_calcs AS (
    SELECT cte_prior_game_log.* 
    FROM cte_prior_game_log
    INNER JOIN cte_tomorrow_game_log ON cte_prior_game_log.player_id = cte_tomorrow_game_log.player_id
    
    UNION ALL 
    
    SELECT * FROM cte_tomorrow_game_log
),


-- A cte calculating rolling averages and sums at the player level
cte_rolling_calcs AS (
    SELECT 
        game_date,
        game_id,
        player_id,
        home_away,
        MAX(position) OVER(PARTITION BY player_id) AS position, 
        MAX(weight_kg) OVER(PARTITION BY player_id) AS weight_kg, 
        MAX(height_cm) OVER(PARTITION BY player_id) AS height_cm, 
        MAX(age) OVER(PARTITION BY player_id) AS age,
        SUM(CASE WHEN wl = 'W' THEN 1 ELSE 0 END) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS win_count,
        SUM(CASE WHEN wl = 'L' THEN 1 ELSE 0 END) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS lose_count,
        AVG(min) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_min,
        SUM(fgm) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fgm,
        AVG(fgm) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fgm,
        SUM(fga) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fga,
        AVG(fga) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fga,
        SUM(fg3_m) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fg3m,
        AVG(fg3_m) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fg3m,
        SUM(fg3_a) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fg3a,
        AVG(fg3_a) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fg3a,
        SUM(ftm) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS ftm,
        AVG(ftm) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_ftm,
        SUM(fta) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fta,
        AVG(fta) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fta,
        AVG(oreb) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_oreb,
        AVG(dreb) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_dreb,
        AVG(reb) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_reb,
        AVG(ast) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_ast,
        AVG(stl) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_stl,
        AVG(blk) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_blk,
        AVG(pf) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_pf,
        AVG(plus_minus) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_plus_minus, 
        AVG(pts) OVER(PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_pts 
    FROM cte_pre_rolling_calcs
)


-- Final query selecting relevant columns, rows (ie tomorrow's games) and calculating percentages
SELECT 
    cte_rolling_calcs.game_date,
    game_id,
    player_id,
    position,
    home_away,
    weight_kg,
    height_cm,
    age,
    win_count, 
    lose_count,
    avg_min,
    avg_fgm,
    avg_fga,
    CASE WHEN fga = 0 THEN 0 ELSE fgm / fga END AS fg_pct,
    avg_fg3m,
    avg_fg3a,
    CASE WHEN fg3a = 0 THEN 0 ELSE fg3m / fg3a END AS fg3_pct,
    avg_ftm,
    avg_fta,
    CASE WHEN fta = 0 THEN 0 ELSE ftm / fta END AS ft_pct,
    avg_oreb,
    avg_dreb,
    avg_reb,
    avg_ast,
    avg_stl,
    avg_blk,
    avg_pf,
    avg_plus_minus,
    avg_pts
FROM cte_rolling_calcs
INNER JOIN (SELECT DISTINCT game_date FROM cte_tomorrow_game_log) AS date_limit
    ON cte_rolling_calcs.game_date = date_limit.game_date