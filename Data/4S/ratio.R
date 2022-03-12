library(tidyverse)
setwd("~/RIDIR/Data/4S")

getRatio = function(data){
  s5 = enframe(read_lines(data), value="line")
  
  d5 = s5 %>% filter(str_detect(line, 'nEdges*')) %>%
    separate(sep = "\\|", col = "line", into = c(NA,NA,"appId",NA,"N"), extra = "drop") %>%
    separate(sep = "=", col = "N", into = c("layer", "n")) %>%
    mutate(n = as.numeric(n)) %>%
    pivot_wider(names_from = layer, values_from = n, values_fill = 0) %>%
    group_by(appId) %>% summarise(edgesA = sum(nEdgesA), edgesB = sum(nEdgesB)) 
  a5 = mean(d5$edgesA)
  print(paste(data, "A:", floor(a5)))
  b5 = mean(d5$edgesB)
  print(paste(data, "B:", floor(b5)))
  ratio = a5 / b5
  print(paste(data, "ratio:", ratio))
  return(ratio)
}

r5 = getRatio("S4/perf5M.txt")
r3 = getRatio("S3/perf3M.txt")
r2 = getRatio("S2/perf2M.txt")
r1 = getRatio("S1/perf1M.txt")
