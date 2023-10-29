
library(nba.dataRub)
library(dplyr)
library(ggplot2)
library(DT)


server <- function(input, output, session){

# Datasets ----------------------------------------------------------------

  # db_con <- if(Sys.info()["nodename"] == "Olivers-MacBook-Pro.local") dh_createCon("postgres") else dh_createCon("cockroach") 
  db_con <- dh_createCon("cockroach")
  df_preds <- dh_getQuery(db_con, "SELECT * FROM anl.pts_prediction") |> 
    mutate(pred_bin = cut(pts_prediction, seq(0, 100, 10), ordered_result = TRUE))


# Update dynamic widgets --------------------------------------------------

   observe({
    # Model Performance tab
    updateSelectInput(session, "pred_bin_select", selected = "All", choices = c("All", as.character(df_preds$pred_bin)))
  })


  output$pts_predictions <- renderDT({
    
    df_preds |> 
      select(game_date, player, team = team_slug, opponent, pts_prediction, pts_actual) |> 
      arrange(desc(game_date), team, player) |> 
      mutate(
        pts_prediction = round(pts_prediction),
        diff = abs(pts_prediction - as.numeric(pts_actual))
      )
    
  }, options = list(filter = NULL))
  
  
  output$model_monitoring_plot <- renderPlot({
    
    sbtl <- if(input$pred_bin_select == "All") ", for all predictions"
      else paste(", for predictions between", input$pred_bin_select, "points")
    
    df <- if(input$pred_bin_select == "All") df_preds
      else filter(df_preds, pred_bin == input$pred_bin_select)
    
    df <- filter(df, pts_actual != "did not play") |> 
      summarise(
        mae = yardstick::mae_vec(pts_prediction, as.numeric(pts_actual)),
        mae_se = sd(pts_prediction) / sqrt(n()),
        .by = game_date
      )
    
    ggplot(df, aes(x = game_date, y = mae)) +
      geom_point(colour = "blue") +
      geom_path(colour = "blue") +
      geom_errorbar(aes(ymin = mae - mae_se, ymax = mae + mae_se), width = 0.1) +
      ylim(c(0, 15)) +
      theme_bw() +
      labs(title = "Points Prediction Model", subtitle = paste0("Model Performance Tracking", sbtl), x = NULL, y = "Mean Absolute Error")
    
    
  })
  
  
}