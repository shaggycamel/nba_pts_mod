
# Libraries ---------------------------------------------------------------

library(shiny)
library(shinydashboard)
library(DT)


# ui ----------------------------------------------------------------------

# Header
header <- dashboardHeader(title = "Point Predictions")
  
# Sidebar
sidebar <- dashboardSidebar(
  sidebarMenu(
    menuItem("Predictions", tabName = "predictions", icon = icon("gears")),
    menuItem("Model Monitoring", tabName = "model_monitoring", icon = icon("arrow-trend-up"))
  )
)
    
# Body
body <- dashboardBody(
    tabItems(

# Predictions -------------------------------------------------------------

      tabItem(tabName = "predictions",
        fluidRow(column(width = 12, DTOutput("pts_predictions")))
      ),

# Model Monitoring ------------------------------------------------------

      tabItem(tabName = "model_monitoring",
        fluidRow(
          column(
            width = 2,
            selectInput("pred_bin_select", "Prediction Bin", choices = character(0))
          ),

          # Plot
          column(width = 10, plotOutput("model_monitoring_plot", height = 600)) # unsure how to make height dynamic, as in = "100%"
        )
      )
    )
  )

# Instantiate page --------------------------------------------------------

ui <- dashboardPage(header, sidebar, body)

