
library(nba.dataRub)
library(tidyverse)
library(tidymodels)
library(here)

db_con <- dh_createCon("cockroach")

# Read model --------------------------------------------------------------

best_fit <- readRDS("best_fit.RDS")


# New data ----------------------------------------------------------------

df_pred <- dh_getQuery(db_con, "pred_prep.sql")


# Prediction --------------------------------------------------------------

df_pred <- mutate(df_pred, next_pts_pred = predict(best_fit, df_pred)[[1]]) |> 
  left_join(
    dh_getQuery(db_con, "SELECT DISTINCT player, player_id FROM nba.team_roster"),
    by = join_by(player_id)
  ) |> 
  relocate(player, .before = player_id)


# Ingestion ---------------------------------------------------------------

# Obtain game_id, player_id, pts_predictions and pts (to come)
df_ingest <- dh_getQuery(db_con, "ingest_dataset.sql") |> 
  left_join(
    select(df_pred, team_slug, game_date, game_id, player_id, next_pts_pred),
    by = join_by(team == team_slug, game_id)
  ) |> 
  select(team, player_id, game_id, next_game_id, next_pts_pred) |> 
  left_join(
    dh_getQuery(db_con, "SELECT player_id, game_id, pts FROM nba.player_game_log WHERE game_date >= '2023-10-20'"),
    by = join_by(player_id, next_game_id == game_id)
  )

DBI::dbWriteTable(db_con, Id(schema = "anl", table = "pts_prediction"), df_ingest, append = TRUE)
