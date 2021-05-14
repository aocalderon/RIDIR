library(tidyverse)

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
  filter(status == "SUCCESS") %>% 
  mutate(node = paste0(host,":",executorId)) %>%
  #select(appId, index, runtime, node, shuffleBytesRead, shuffleRecordsRead) %>%
  select(appId, index, runtime, shuffleBytesRead, shuffleRecordsRead)

#source("JSONReader.R")
#ids = c(2, 4, 6, 8, 10, 12, 14)
#getCellInfo(ids)
cells = read_tsv("cells.tsv")

data = spark %>% inner_join(tasks, by = c("appId")) %>%
  group_by(tag, index) %>% 
  summarise(runtime = mean(runtime), bytes = mean(shuffleBytesRead), records = mean(shuffleRecordsRead))

wkt = data %>% inner_join(cells, by = c("tag", "index"))

tags = c(2,4,6,8,10,12,14)
for(tag in tags){
  t = paste0("P",tag,"K")
  wkt %>% filter(tag == t) %>%
    write_tsv(paste0("cells/cells",t,".wkt"))
}
