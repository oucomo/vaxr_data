library(arrow)
library(tidyverse)

# custom summary function for arrow object  
arrow_summary <- function(data, var, ...){
  data %>%  summarize(
    min_value = min({{var}}, na.rm=TRUE),
    first_quantile = quantile({{var}}, probs = (0.25), na.rm=TRUE),
    mean_value = mean({{var}}, na.rm=TRUE),
    median_value = median({{var}}, na.rm=TRUE),
    third_quantile = quantile({{var}}, probs = (0.75), na.rm=TRUE),
    max_value = max({{var}}, na.rm=TRUE),
    sd_value = sd({{var}}, na.rm=TRUE)
  )
}