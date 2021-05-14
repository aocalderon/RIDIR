library(tidyverse)
library(ggplot2)

plotCores <- function(appId){
  cores <- read_tsv(paste0("/tmp/cores/c", appId, ".csv"), 
                    col_names = FALSE, col_types = cols(X1 = col_datetime(format = "%Y-%m-%d %H:%M:%S")))
  names(cores) = c("times", "cores")
  p = ggplot(data = cores, aes(x = times, y = cores)) +
    geom_line()
  ggsave(paste0("cores/cores", appId, ".pdf"), width = 12, height = 8)  
}

for(appId in 468:488){
  plotCores(appId)  
}

