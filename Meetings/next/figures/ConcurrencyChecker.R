library(tidyverse)
library(lubridate)

paramsPattern = "quadtree"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}
getTag <- function(quadtree){
  tag = str_split(quadtree, "/")[[1]]
  tag = tag[grepl("edges", tag)]
  tag = str_split(tag, "_")[[1]]
  tag = tag[grepl("P", tag)]
  return(paste(tag, collapse = ""))
}
spark = enframe(readLines("tasks.txt")) %>% 
  filter(grepl(value, pattern = "SparkSubmit ")) %>%
  separate(value, into = c(NA, NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"qtag"), sep = " ") %>%
  select(appId, qtag) 
spark$tag = spark$qtag %>% map(getTag)
spark = spark %>% select(appId, tag) %>% unnest(tag)

tasks = read_tsv("tasks.tsv") %>% 
  select(appId, index, host, executorId, launchTime, duration) %>%
  mutate(node = paste0(host,":",executorId)) %>%
  inner_join(spark, by = c("appId")) %>%
  select(appId, node, index, launchTime, duration)

test = tasks %>% filter(appId == "application_1615435002078_0468") %>% 
  mutate(start = as.numeric(as.POSIXct(as_datetime(launchTime))) * 1000) %>%
  mutate(end = start + duration) 

getInterval <- function(params){
  row = tibble(taskId = params[1], interval = params[2]:params[3])
  return(row)
}
