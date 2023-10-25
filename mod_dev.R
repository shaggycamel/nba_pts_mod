
library(nba.dataRub)
library(tidyverse)
library(tidymodels)
library(baguette)
library(doParallel)


df_raw <- dh_getQuery(dh_createCon("postgres"), "SELECT * FROM nba.player_game_log WHERE season_type != 'All Star'") |> 
  mutate(
    next_pts = lead(pts, order_by = game_date), 
    # team = str_sub(matchup, 1, 3),
    # opponent = str_sub(matchup, str_locate(matchup, " @ | vs. ")[, "end"]),
    # next_opponent = lead(opponent, order_by = game_date),
    season_type = as.numeric(ordered(season_type, levels = c("Pre Season", "Regular Season", "Playoffs"))),
    across(c(where(is.numeric), -next_pts), \(x) replace_na(x, 0)),
    .by = player_id
  ) |> 
  filter(!is.na(next_pts))



# Split Data --------------------------------------------------------------

df_split <- initial_split(df_raw)
df_train <- training(df_split)
df_test <- testing(df_split)


# Recipe ------------------------------------------------------------------

rec <- recipe(data = df_train, next_pts ~ .) |> 
  update_role(player_id, game_id, new_role = "id") |> 
  step_rm(year_season, slug_season, video_available, matchup, game_date, wl) |> 
  step_naomit(everything()) |> 
  # probably should have a log step here too
  step_range(c(all_numeric_predictors(), -season_type)) |> 
  step_dummy(all_factor_predictors()) 
  
  
# view(x <- bake(prep(rec), new_data = NULL))
recs <- mget(str_subset(objects(), "rec$"))


# Model Definitions -------------------------------------------------------

# TAKE TOO LONG TO PROCESS 
# boost_tree_xgboost_spec <- boost_tree(tree_depth = 30, trees = 500, learn_rate = tune(), min_n = 30, loss_reduction = tune(), sample_size = tune(), stop_iter = 5) |>
#   set_engine("xgboost") |>
#   set_mode("regression")
# 
# cubist_rules_Cubist_spec <- cubist_rules(committees = tune(), neighbors = tune(), max_rules = tune()) |>
#   set_engine("Cubist")

bag_mars_earth_spec <- bag_mars() |>
  set_engine("earth") |>
  set_mode("regression")

linear_reg_glm_spec <- linear_reg() |>
  set_engine("glm")

decision_tree_rpart_spec <- decision_tree(tree_depth = tune(), min_n = 30, cost_complexity = tune()) |>
  set_engine("rpart") |>
  set_mode("regression")

rand_forest_ranger_spec <- rand_forest(mtry = tune(), min_n = tune()) |>
  set_engine("ranger") |>
  set_mode("regression")

mods <- mget(str_subset(objects(), "_spec$"))


# Workflow ----------------------------------------------------------------

# Register parallel backend
# registerDoParallel(cores = parallel::detectCores(logical = FALSE))
# 
# # Tune models
# cv_folds <- vfold_cv(df_train, v = 5)
# wflows <- workflow_set(recs, mods) |>
#   workflow_map(
#     "tune_grid",
#     resamples = cv_folds,
#     grid = 10,
#     verbose = TRUE,
#     metrics = metric_set(mae, rmse, rsq),
#     control = control_grid(parallel_over = "everything", verbose = TRUE)
#   )
# 
# # Drop parallel (ie, re-register sequential processing)
# registerDoSEQ()
# save.image()


# Model Comparison --------------------------------------------------------

autoplot(wflows)
rank_results(wflows)


# Fit best model ----------------------------------------------------------

mod_type <- "rec_rand_forest_ranger_spec"
best_wflow <- extract_workflow(wflows, mod_type)

best_mod <- wflows |> 
  extract_workflow_set_result(mod_type) |> 
  select_best(metric = "mae")
  
best_fit <- finalize_workflow(best_wflow, best_mod) |> 
  fit(data = df_train)


# Test predictions --------------------------------------------------------

df_test <- df_test |> 
  mutate(
    preds = predict(best_fit, df_test)[[1]],
    pred_error = next_pts - preds
  )


# Results -----------------------------------------------------------------

mae(df_test, next_pts, preds)
hist(df_test$pred_error)

pivot_longer(df_test, c(next_pts, preds), names_to = "dist") |> 
  ggplot(aes(x = value, fill = dist)) +
  geom_density(alpha = 0.3) +
  geom_vline(xintercept = 20)


# Extract model -----------------------------------------------------------

butcher::weigh(best_fit)
saveRDS(best_fit, "best_fit.RDS")
