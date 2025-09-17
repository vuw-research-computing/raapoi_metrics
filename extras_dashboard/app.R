
library(shiny)
library(ggplot2)
library(dplyr)
library(readr)
library(plotly)
library(stringr)
library(scales)

# Plotting function for CPU, Memory, GPU usage
generate_plot_output <- function(data, title, y_config, y_alloc, y_label) {
  data <- data %>%
    mutate(hover_text = paste(
      "Time:", format(Timestamp, "%b %d, %H:%M"), "<br>",
      "Configured:", .data[[y_config]], "<br>",
      "Allocated:", .data[[y_alloc]]
    ))

  p <- ggplot(data, aes(x = Timestamp)) +
    geom_line(aes(y = .data[[y_config]], color = "Configured", text = hover_text)) +
    geom_area(aes(y = .data[[y_alloc]], fill = "Allocated"), alpha = 0.4) +
    scale_fill_manual(name = "Status", values = c("Allocated" = "cadetblue")) +
    scale_color_manual(name = "Status", values = c("Configured" = "darkblue")) +
    scale_x_datetime(date_labels = "%b %d, %H:%M") +
    labs(title = title, y = y_label, x = "Time") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "bottom")

  ggplotly(p, tooltip = "text")
}

# Donut chart function
generate_donut_chart <- function(data, title, cfg_entity, alloc_entity) {
  if (!(cfg_entity %in% names(data) && alloc_entity %in% names(data))) return(NULL)

  last_row <- tail(data, 1)
  total <- last_row[[cfg_entity]]
  used <- last_row[[alloc_entity]]
  if (is.na(total) || total == 0) return(NULL)

  available <- total - used
  plot_ly(
    labels = c("Available", "Used"),
    values = c(available, used),
    type = 'pie',
    hole = 0.6,
    marker = list(colors = c('lightblue', 'cadetgreen')),
    textinfo = 'label+percent',
    insidetextfont = list(color = '#FFFFFF'),
    hoverinfo = 'label+value',
    title = title,
    showlegend = FALSE
  )
}

# UI
ui <- navbarPage(
  title = "Cluster Monitoring Dashboard",

  tabPanel("Queue", mainPanel(
    uiOutput("queue_timestamp_info"),
    plotlyOutput("queuePlot", height = "400px"))),

  tabPanel("CPU",
    sidebarLayout(
      sidebarPanel(
        selectInput("cpu_file", "Select CPU File:", choices = NULL),
        uiOutput("cpu_timestamp_slider")
      ),
      mainPanel(
        uiOutput("cpu_timestamp_info"),
        plotlyOutput("cpuPlot", height = "400px"),
        plotlyOutput("cpuDonutPlot", height = "400px")
      )
    )
  ),

  tabPanel("CPU Donut Charts", fluidRow(uiOutput("cpu_donut_charts"))),

  tabPanel("Memory",
    sidebarLayout(
      sidebarPanel(
        selectInput("mem_file", "Select Memory File:", choices = NULL),
        uiOutput("mem_timestamp_slider")
      ),
      mainPanel(
        uiOutput("mem_timestamp_info"),
        plotlyOutput("memPlot", height = "400px"),
        plotlyOutput("memDonutPlot", height = "400px")
      )
    )
  ),

  tabPanel("Memory Donut Charts", fluidRow(uiOutput("mem_donut_charts"))),

  tabPanel("GPU",
    sidebarLayout(
      sidebarPanel(
        selectInput("gpu_file", "Select GPU File:", choices = NULL),
        uiOutput("gpu_timestamp_slider")
      ),
      mainPanel(
        uiOutput("gpu_timestamp_info"),
        plotlyOutput("gpuUtilPlot", height = "400px"),
        plotlyOutput("gpuMemPlot", height = "400px"),
      )
    )
  ),

  tabPanel("GPU Donut Charts", fluidRow(uiOutput("gpu_donut_charts")))
)

# Server
server <- function(input, output, session) {
  # File watchers
  cpu_files <- reactive({
    list.files("node_data", pattern = "\\.csv$", full.names = TRUE)
  })

  gpu_files <- reactive({
    list.files("gpu_data", pattern = "^gpu_stats_.*\\.csv$", full.names = TRUE)
  })

  output$cpu_timestamp_info <- renderUI({
    df <- cpu_data()
    if (nrow(df) > 0) {
      last_ts <- tail(df$Timestamp, 1)
      HTML(paste0("<b>Last CPU Update:</b> ", format(last_ts, "%Y-%m-%d %H:%M:%S")))
    }
  })

  output$mem_timestamp_info <- renderUI({
    df <- mem_data()
    if (nrow(df) > 0) {
      last_ts <- tail(df$Timestamp, 1)
      HTML(paste0("<b>Last Memory Update:</b> ", format(last_ts, "%Y-%m-%d %H:%M:%S")))
    }
  })

  output$gpu_timestamp_info <- renderUI({
    df <- gpu_data()
    if (nrow(df) > 0) {
      last_ts <- tail(df$Timestamp, 1)
      HTML(paste0("<b>Last GPU Update:</b> ", format(last_ts, "%Y-%m-%d %H:%M:%S")))
    }
  })

  output$queue_timestamp_info <- renderUI({
    df <- queue_data()
    if (nrow(df) > 0) {
      last_ts <- tail(df$Timestamp, 1)
      HTML(paste0("<b>Last Queue Update:</b> ", format(last_ts, "%Y-%m-%d %H:%M:%S")))
    }
  })


  # Reactive CPU/memory data
  cpu_data_list <- reactive({
    lapply(cpu_files(), function(f) {
      list(file_name = basename(f), data = reactiveFileReader(5000, session, f, read_csv, show_col_types = FALSE))
    })
  })

  gpu_data_list <- reactive({
    lapply(gpu_files(), function(f) {
      list(file_name = basename(f), data = reactiveFileReader(5000, session, f, read_csv, show_col_types = FALSE))
    })
  })

  # Update dropdowns
  observe({
    updateSelectInput(session, "cpu_file", choices = sapply(cpu_data_list(), `[[`, "file_name"))
    updateSelectInput(session, "mem_file", choices = sapply(cpu_data_list(), `[[`, "file_name"))
    updateSelectInput(session, "gpu_file", choices = sapply(gpu_data_list(), `[[`, "file_name"))
  })

  cpu_data <- reactive({
    req(input$cpu_file)
    df <- Filter(function(x) x$file_name == input$cpu_file, cpu_data_list())[[1]]$data()
    df$Timestamp <- as.POSIXct(df$Timestamp)
    df
  })

  mem_data <- reactive({
    req(input$mem_file)
    df <- Filter(function(x) x$file_name == input$mem_file, cpu_data_list())[[1]]$data()
    df$Timestamp <- as.POSIXct(df$Timestamp)
    df$Cfgmem <- df$Cfgmem/1024
    df$Allocmem <- df$Allocmem/1024
    df
  })

  gpu_data <- reactive({
    req(input$gpu_file)
    df <- Filter(function(x) x$file_name == input$gpu_file, gpu_data_list())[[1]]$data()
    df$Timestamp <- as.POSIXct(df$Timestamp)
    df
  })

  queue_data <- reactiveFileReader(5000, session, "queue.csv", function(f) {
    read_csv(f, show_col_types = FALSE) %>% mutate(Timestamp = as.POSIXct(Timestamp))
  })

  # Timestamp sliders
  output$cpu_timestamp_slider <- renderUI({
    df <- cpu_data()
    sliderInput("cpu_time_range", "Select Time Range:",
      min = min(df$Timestamp), max = max(df$Timestamp),
      value = c(min(df$Timestamp), max(df$Timestamp)),
      timeFormat = "%Y-%m-%d %H:%M:%S"
    )
  })

  output$mem_timestamp_slider <- renderUI({
    df <- mem_data()
    sliderInput("mem_time_range", "Select Time Range:",
      min = min(df$Timestamp), max = max(df$Timestamp),
      value = c(min(df$Timestamp), max(df$Timestamp)),
      timeFormat = "%Y-%m-%d %H:%M:%S"
    )
  })

  output$gpu_timestamp_slider <- renderUI({
    df <- gpu_data()
    sliderInput("gpu_time_range", "Select Time Range:",
      min = min(df$Timestamp), max = max(df$Timestamp),
      value = c(min(df$Timestamp), max(df$Timestamp)),
      timeFormat = "%Y-%m-%d %H:%M:%S"
    )
  })

  # Filtered data
  filtered_cpu_data <- reactive({
    df <- cpu_data()
    req(input$cpu_time_range)
    df %>% filter(Timestamp >= input$cpu_time_range[1], Timestamp <= input$cpu_time_range[2])
  })

  filtered_mem_data <- reactive({
    df <- mem_data()
    req(input$mem_time_range)
    df %>% filter(Timestamp >= input$mem_time_range[1], Timestamp <= input$mem_time_range[2])
  })

  filtered_gpu_data <- reactive({
    df <- gpu_data()
    req(input$gpu_time_range)
    df %>% filter(Timestamp >= input$gpu_time_range[1], Timestamp <= input$gpu_time_range[2])
  })

  # Main Plots
  # TODO: Fix PENDING hover info
  output$queuePlot <- renderPlotly({
    df <- queue_data()
    p <- ggplot(df, aes(x = Timestamp)) +
      geom_line(aes(y = Running, color = "Running")) +
      geom_area(aes(y = Pending, fill = "Pending"), alpha = 0.4) +
      scale_fill_manual(values = c("Pending" = "red")) +
      scale_color_manual(values = c("Running" = "lightblue")) +
      labs(title = "Queue Status", y = "Jobs", x = "Time") +
      theme_minimal() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
    ggplotly(p)
  })

  output$cpuPlot <- renderPlotly({
    generate_plot_output(filtered_cpu_data(), "CPU Allocation", "Cfgcpu", "Alloccpu", "CPU")
  })

  output$memPlot <- renderPlotly({
    generate_plot_output(filtered_mem_data(), "Memory Allocation", "Cfgmem", "Allocmem", "Memory")
  })

  output$gpuUtilPlot <- renderPlotly({
    df <- filtered_gpu_data()
    req("Utilization_GPU" %in% names(df))
    p <- ggplot(df, aes(x = Timestamp, y = Utilization_GPU, color = factor(Index))) +
      geom_line() +
      labs(title = "GPU Utilization", y = "%", x = "Time") +
      theme_minimal() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
    ggplotly(p)
  })

  output$gpuMemPlot <- renderPlotly({
    df <- filtered_gpu_data()
    req(all(c("Memory_Used", "Memory_Total") %in% names(df)))
    p <- ggplot(df, aes(x = Timestamp, color = factor(Index))) +
      geom_area(aes(y = Memory_Used, fill = factor(Index)), alpha = 0.4) +
      geom_line(aes(y = Memory_Total), linetype = "dashed") +
      labs(title = "GPU Memory Usage", y = "MiB", x = "Time") +
      theme_minimal() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
    ggplotly(p)
  })

  # Donut chart outputs
  output$cpuDonutPlot <- renderPlotly({
    generate_donut_chart(cpu_data(), "CPU Allocation", "Cfgcpu", "Alloccpu")
  })

  output$memDonutPlot <- renderPlotly({
    generate_donut_chart(mem_data(), "Memory Allocation", "Cfgmem", "Allocmem")
  })

  output$gpuDonutPlot <- renderPlotly({
    generate_donut_chart(gpu_data(), "GPU Allocation", "Cfggres/gpu", "Allocgres/gpu")
  })

  # CPU donut chart grid
  output$cpu_donut_charts <- renderUI({
    charts <- lapply(cpu_data_list(), function(file_info) {
      df <- file_info$data()
      df$Timestamp <- as.POSIXct(df$Timestamp)

      # Unique IDs for plots
      plot_id <- paste0("cpuDonut_", tools::file_path_sans_ext(file_info$file_name))
      date_id <- paste0("cpuDate_", tools::file_path_sans_ext(file_info$file_name))

      # Render the plot
      output[[plot_id]] <- renderPlotly({
        title <- str_extract(file_info$file_name, "[^_]+")
        generate_donut_chart(df, title, "Cfgcpu", "Alloccpu")
      })

      # Render timestamp below chart
      output[[date_id]] <- renderUI({
        if (!is.null(df$Timestamp)) {
          last_ts <- tail(df$Timestamp, 1)
          HTML(paste0(" ", format(last_ts, "%Y-%m-%d %H:%M:%S")))
        }
      })

      # Output layout
      column(3,
        div(style = "text-align: center;",
          plotlyOutput(plot_id, height = "200px"),
          uiOutput(date_id)
        ))
    })
    do.call(fluidRow, charts)
  })

  # Memory donut chart grid
  output$mem_donut_charts <- renderUI({
    charts <- lapply(cpu_data_list(), function(file_info) {
      df <- file_info$data()
      df$Timestamp <- as.POSIXct(df$Timestamp)

      # Unique IDs for plots
      plot_id <- paste0("memDonut_", tools::file_path_sans_ext(file_info$file_name))
      date_id <- paste0("memDate_", tools::file_path_sans_ext(file_info$file_name))

      # Render the plot
      output[[plot_id]] <- renderPlotly({
        title <- str_extract(file_info$file_name, "[^_]+")
        generate_donut_chart(df, title, "Cfgmem", "Allocmem")
      })

      # Render timestamp below chart
      output[[date_id]] <- renderUI({
        if (!is.null(df$Timestamp)) {
          last_ts <- tail(df$Timestamp, 1)
          HTML(paste0(" ", format(last_ts, "%Y-%m-%d %H:%M:%S")))
        }
      })

      # Output layout
      column(3,
        div(style = "text-align: center;",
          plotlyOutput(plot_id, height = "200px"),
          uiOutput(date_id)
        ))
    })
    do.call(fluidRow, charts)
  })

  # GPU donut chart grid (filtered by file name)
  output$gpu_donut_charts <- renderUI({
    charts <- lapply(cpu_data_list(), function(file_info) {
      if (grepl("gpu|high", file_info$file_name, ignore.case = TRUE)) {
        df <- file_info$data()
        df$Timestamp <- as.POSIXct(df$Timestamp)

        # Unique IDs for plots
        plot_id <- paste0("gpuDonut_", tools::file_path_sans_ext(file_info$file_name))
        date_id <- paste0("gpuDate_", tools::file_path_sans_ext(file_info$file_name))

        # Render the plot
        output[[plot_id]] <- renderPlotly({
          title <- str_extract(file_info$file_name, "[^_]+")
          generate_donut_chart(df, title, "Cfggres/gpu", "Allocgres/gpu")
        })

        # Render timestamp below chart
        output[[date_id]] <- renderUI({
          if (!is.null(df$Timestamp)) {
            last_ts <- tail(df$Timestamp, 1)
            HTML(paste0(" ", format(last_ts, "%Y-%m-%d %H:%M:%S")))
          }
        })

        # Output layout
        column(3,
          div(style = "text-align: center;",
            plotlyOutput(plot_id, height = "200px"),
            uiOutput(date_id)
          ))
      }
    })
    do.call(fluidRow, charts)
  })
}

shinyApp(ui, server)

