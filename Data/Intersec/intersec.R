library(tidyverse)
setwd("~/RIDIR/Data/Intersec/")

data0 = enframe(read_lines("intersec.txt"), value="line")

data1 = data0 %>%
  filter(str_detect(line, 'INFO')) 

fields1 = c("ts","t","tag","runId","n1","n2","max","sum","len","avg")
data2 = data1 %>% 
  separate(sep = "\\|", col = "line", into = fields1, extra = "drop") %>%
  mutate(max = as.numeric(max), avg = as.numeric(avg), len = as.numeric(len))


counts = data2 %>% mutate(sameCount = n1 == n2) %>% select(sameCount) 
print(paste0("Both report the same number of intersections? ", length( unique( counts[,1] ) ) == 1) )

n = data2 %>% select(len) %>% summarise(len = mean(len))
print(paste0("The average number of intersections is ", n))

max = data2 %>% select(max) %>% summarise(max = mean(max))
print(paste0("The maximun distance difference was ", max))

avg = data2 %>% select(avg) %>% summarise(avg = mean(avg))
print(paste0("The average distance difference was ", avg))
