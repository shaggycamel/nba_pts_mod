
WITH cte_season_player_team AS (
    SELECT *
    FROM (
        SELECT 
            slug_season, 
            player_id, 
            team_slug,
            CASE 
                WHEN position = 'F-C' THEN 'C-F'
                WHEN position = 'G-F' THEN 'F-G'
                ELSE position
            END AS position, 
            weight_kg, 
            height_cm, 
            age, 
            ROW_NUMBER() OVER(PARTITION BY slug_season, player_id ORDER BY how_acquired DESC) AS rn
        FROM nba.team_roster
    ) AS latest_team
    WHERE rn = 1
),

cte_rolling_calcs AS (
    SELECT 
        log.game_date,
        log.game_id,
        log.player_id,
        REPLACE(REGEXP_REPLACE(log.matchup, ' vs. | @ ', '', 'gi'), cte_season_player_team.team_slug, '') AS opponent, 
        SUM(CASE WHEN log.wl = 'W' THEN 1 ELSE 0 END) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS win_count,
        SUM(CASE WHEN log.wl = 'L' THEN 1 ELSE 0 END) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS lose_count,
        AVG(log.min) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_min,
        SUM(log.fgm) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fgm,
        AVG(log.fgm) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fgm,
        SUM(log.fga) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fga,
        AVG(log.fga) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fga,
        SUM(log.fg3_m) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fg3m,
        AVG(log.fg3_m) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fg3m,
        SUM(log.fg3_a) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fg3a,
        AVG(log.fg3_a) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fg3a,
        SUM(log.ftm) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS ftm,
        AVG(log.ftm) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_ftm,
        SUM(log.fta) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS fta,
        AVG(log.fta) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_fta,
        AVG(log.oreb) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_oreb,
        AVG(log.dreb) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_dreb,
        AVG(log.reb) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_reb,
        AVG(log.ast) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_ast,
        AVG(log.stl) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_stl,
        AVG(log.blk) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_blk,
        AVG(log.pf) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_pf,
        AVG(log.plus_minus) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_plus_minus, 
        AVG(log.pts) OVER(PARTITION BY log.player_id ORDER BY log.game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS avg_pts, 
        cte_season_player_team.team_slug, 
        cte_season_player_team.position, 
        cte_season_player_team.weight_kg, 
        cte_season_player_team.height_cm, 
        cte_season_player_team.age,
        log.pts >= 20 AS over_20_pts
    FROM nba.player_game_log AS log
    INNER JOIN cte_season_player_team ON log.player_id = cte_season_player_team.player_id
        AND log.slug_season = cte_season_player_team.slug_season
    WHERE log.slug_season < '2023-24'
)

SELECT 
    game_date,
    game_id,
    player_id,
--    team_slug,
    position,
    weight_kg,
    height_cm,
    age,
--    opponent,
    win_count / (CASE WHEN lose_count = 0 THEN 1 ELSE lose_count END) AS wl_ratio,
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
    avg_pts,
    over_20_pts
FROM cte_rolling_calcs
WHERE avg_pts IS NOT NULL

ORDER BY player_id, game_date

