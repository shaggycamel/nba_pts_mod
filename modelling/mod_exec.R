
library(nba.dataRub)
library(dplyr)
library(stringr)
library(workflows)
library(DBI)

db_con <- dh_createCon("cockroach")
best_fit <- readRDS("best_fit.RDS")
game_date <- as.character(as.Date(Sys.Date(), tz = "NZ") - 1)


# Has latest data been collected? -----------------------------------------

if(nrow(dbGetQuery(db_con, glue::glue(readr::read_file(here::here("queries", "check_update_log.sql"))))) > 0){
  stop("nba.player_game_log has not been updated. Model execution will cease...")
}


# New Predictions ---------------------------------------------------------

df_pred <- na.omit(dh_getQuery(db_con, "pred_prep.sql"))

if(nrow(df_pred) > 0) {
  # Need to break these steps up so df_pred can be referenced in predict
  df_pred <- mutate(df_pred, next_pts_pred = predict(best_fit, df_pred)[[1]]) |> 
    left_join(dh_getQuery(db_con, "post_pred_clean.sql"), by = join_by(player_id)) |> 
    left_join(dh_getQuery(db_con, "SELECT game_id, slug_matchup FROM nba.league_game_schedule")) |> 
    mutate(
      opponent = str_remove(str_remove(slug_matchup, team_slug), " @ | vs. "),
      pts_actual = NA_character_, # set as char so "did not play" can be included
    ) |>
    relocate(c(player, team_slug, opponent), .after = player_id) |> 
    select(-slug_matchup, pts_prediction = next_pts_pred)
} else {
  cat("No games being played...nothing to predict")
}

# Run ingestion step last so df_actual query can work (WHERE pts_actual IS NULL)


# Altering existing NA predictions ----------------------------------------

df_actual <- dh_getQuery(db_con, "SELECT * FROM anl.pts_prediction WHERE pts_actual IS NULL")

game_ids <- paste(unique(df_actual$game_id), collapse = ", ")
actuals <- dh_getQuery(db_con, "SELECT * FROM nba.player_game_log WHERE game_id IN ({game_ids})", game_ids)

df_actual <- select(df_actual, -pts_actual) |> 
  left_join(select(actuals, game_id, player_id, pts_actual = pts)) |> 
  mutate(pts_actual = case_when(
    is.na(pts_actual) ~ "did not play",
    .default = as.character(pts_actual)
  ))


# Ingestion of data -------------------------------------------------------

dbSendQuery(db_con, glue::glue("DELETE FROM anl.pts_prediction WHERE game_id IN ({game_ids})"))
dbWriteTable(db_con, Id(schema = "anl", table = "pts_prediction"), df_actual, append = TRUE)
dbWriteTable(db_con, Id(schema = "anl", table = "pts_prediction"), df_pred, append = TRUE)

print(paste("Added predictions for games played on:", game_date))
