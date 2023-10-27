
library(nba.dataRub)
library(tidyverse)
library(tidymodels)
library(themis)
library(baguette)
library(discrim)
library(doParallel)


# Split Data --------------------------------------------------------------

df_raw <- dh_getQuery(dh_createCon("postgres"), "train.sql") |> 
  mutate(over_20_pts = as.factor(over_20_pts))

df_split <- initial_split(df_raw)
df_train <- training(df_split)
df_test <- testing(df_split)


# Recipe ------------------------------------------------------------------

rec_original <- recipe(data = df_train, over_20_pts ~ .) |> 
  update_role(player_id, game_id, game_date, new_role = "id") |> 
  step_naomit(everything()) |> 
  step_log(c(all_numeric_predictors(), -c(weight_kg, height_cm, avg_min, avg_plus_minus, ends_with("pct"))), offset = 1) |> 
  step_range(all_numeric_predictors()) |> 
  step_pca(all_numeric_predictors()) |> 
  step_dummy(all_factor_predictors())
  
rec_smote <- step_smotenc(rec_original, over_20_pts)

recs <- mget(str_subset(objects(), "^rec_"))

# x <- bake(prep(rec_original), new_data = NULL)
# map(recs, \(x) count(bake(prep(x), new_data = NULL), over_20_pts))


# Model Definitions -------------------------------------------------------

bag_mars_earth_spec <- bag_mars() |>
  set_engine("earth") |>
  set_mode("classification")

# boost_tree_xgboost_spec <- boost_tree(tree_depth = tune(), trees = tune(), learn_rate = tune(), min_n = tune(), loss_reduction = tune(), sample_size = tune(), stop_iter = 2) |>
#   set_engine("xgboost") |>
#   set_mode("classification")

logistic_reg_glmnet_spec <- logistic_reg(penalty = tune(), mixture = tune()) |>
  set_engine("glmnet")

logistic_reg_glm_spec <- logistic_reg() |>
  set_engine("glm")

mods <- mget(str_subset(objects(), "_spec$"))


# Workflow ----------------------------------------------------------------

# Register parallel backend
registerDoParallel(cores = parallel::detectCores(logical = FALSE) / 2)

# Tune models
cv_folds <- vfold_cv(df_train, v = 5, strata = over_20_pts)
wflows <- workflow_set(recs, mods) |>
  workflow_map(
    "tune_grid",
    resamples = cv_folds,
    grid = 10,
    verbose = TRUE,
    metrics = metric_set(npv, bal_accuracy),
    control = control_grid(parallel_over = "everything", verbose = TRUE)
  )

# Drop parallel (ie, re-register sequential processing)
registerDoSEQ()
save.image()


# Model Comparison --------------------------------------------------------

autoplot(wflows)
view(rank_results(wflows))


# Fit best model ----------------------------------------------------------

mod_type <- "rec_original_logistic_reg_glmnet_spec"
best_wflow <- extract_workflow(wflows, mod_type)

mod_metric <- "npv"
best_mod <- wflows |> 
  extract_workflow_set_result(mod_type) |> 
  select_best(metric = mod_metric)
  
best_fit <- finalize_workflow(best_wflow, best_mod) |> 
  fit(data = df_train)


# Test predictions --------------------------------------------------------

df_test <- mutate(df_test, preds = predict(best_fit, df_test)[[1]])

conf_mat(df_test, over_20_pts, preds)
npv(df_test, over_20_pts, preds) # 2216 / (2216 + 717) = 0.76
bal_accuracy(df_test, over_20_pts, preds)



# Extract model -----------------------------------------------------------

butcher::weigh(best_fit)
saveRDS(best_fit, "best_fit.RDS")
