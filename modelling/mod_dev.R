
library(nba.dataRub)
library(tidyverse)
library(tidymodels)
library(baguette)
library(rules)
library(doParallel)



# Data --------------------------------------------------------------------

df_raw <- dh_getQuery(dh_createCon("postgres"), "train.sql")

df_split <- initial_split(df_raw)
df_train <- training(df_split)
df_test <- testing(df_split)


# Recipe ------------------------------------------------------------------

rec_original <- recipe(data = df_train, pts ~ .) |> 
  update_role(player_id, game_id, game_date, new_role = "id") |> 
  step_naomit(everything()) |> 
  step_log(c(all_numeric_predictors(), -c(weight_kg, height_cm, avg_min, avg_plus_minus, ends_with("pct"))), offset = 1) |> 
  step_range(all_numeric_predictors()) |> 
  step_pca(all_numeric_predictors()) |> 
  step_dummy(all_factor_predictors())
  
recs <- mget(str_subset(objects(), "^rec_"))

# x <- bake(prep(rec_original), new_data = NULL)
# map(recs, \(x) count(bake(prep(x), new_data = NULL), over_20_pts))


# Model Definitions -------------------------------------------------------

boost_tree_xgboost_spec <- boost_tree(tree_depth = tune(), trees = tune(), learn_rate = tune(), min_n = tune(), loss_reduction = tune(), sample_size = tune(), stop_iter = 2) |>
  set_engine("xgboost") |>
  set_mode("regression")

linear_reg_glmnet_spec <- linear_reg(penalty = tune(), mixture = tune()) |> 
  set_engine("glmnet")

bag_mars_earth_spec <- bag_mars() |>
  set_engine("earth") |>
  set_mode("regression")

linear_reg_glm_spec <- linear_reg() |>
  set_engine("glm")

decision_tree_rpart_spec <- decision_tree(tree_depth = tune(), min_n = tune(), cost_complexity = tune()) |>
  set_engine("rpart") |>
  set_mode("regression")

mods <- mget(str_subset(objects(), "_spec$"))


# Workflow ----------------------------------------------------------------

# Register parallel backend
registerDoParallel(cores = parallel::detectCores(logical = FALSE) / 2)

# Tune models
cv_folds <- vfold_cv(df_train, v = 5)
wflows <- workflow_set(recs, mods) |>
  workflow_map(
    "tune_grid",
    resamples = cv_folds,
    grid = 10,
    verbose = TRUE,
    metrics = metric_set(mae, rmse, rsq),
    control = control_grid(parallel_over = "everything", verbose = TRUE)
  )

# Drop parallel (ie, re-register sequential processing)
registerDoSEQ()
save.image()


# Model Comparison --------------------------------------------------------

autoplot(wflows)
view(rank_results(wflows))


# Fit best model ----------------------------------------------------------

mod_type <- "rec_original_boost_tree_xgboost_spec"
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
    pred_diff = pts - preds,
    pred_bins = cut(preds, breaks = c(-10, 0, 10, 20, 30, 40))
  )

# Results -----------------------------------------------------------------

mae(df_test, pts, preds)
hist(df_test$pred_diff)

# Model is underpredicting when players score large, and
# forecasted to be in the 20-30 bin
df_test |> 
  group_by(pred_bins) |> 
  summarise(
    bin_mae = sum(abs(preds - pts)) / n(),
    bin_diff = sum(pts - preds)
  )

# Players performing well past the 20 point threshold
filter(df_test, pred_bins == "(20,30]") |> 
  ggplot(aes(x = pts)) +
  geom_histogram(fill = "dodgerblue", colour = "black") +
  geom_vline(xintercept = 20, colour = "red") +
  theme_bw()


# Extract model -----------------------------------------------------------

butcher::weigh(best_fit)
saveRDS(best_fit, "best_fit.RDS")
