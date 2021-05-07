require(tidyverse)
require(rvest)

appId = "application_1615435002078_0439"
stageId = 10
theurl = paste0("http://localhost:18081/history/", appId, "/stages/stage/?id=", stageId, "&attempt=0&task.sort=Duration&task.desc=true&task.pageSize=250")
webpage <- read_html(theurl)
tbls <- html_nodes(webpage, "table")

tasks0 = webpage %>%
  html_nodes("#task-table") %>% 
  html_table(fill = TRUE) %>% .[[1]] %>% select(X1, X2, X6, X7, X8, X9, X16)
names(tasks0) = c("cellId", "taskId", "executor", "host", "launchTime", "duration", "sizeAndRecords")

parseTime <- function(str){
  if(str == ""){
    return(0)
  } 
  arr = str_split(str, " ")
  d = as.numeric(arr[[1]][1])
  if(arr[[1]][2] == "ms"){
    d = d / 1000.0
  }
  return(d)
}

parseSize <- function(str){
  if(str == ""){
    return(0)
  } 
  arr = str_split(str, " ")
  d = as.numeric(arr[[1]][1])
  if(arr[[1]][2] == "KB"){
    d = d * 1e3
  } else if(arr[[1]][2] == "MB"){
    d = d * 1e6
  } 
  return(d)
}

tasks = tasks0 %>% separate(host, into = c("host", NA, NA), sep = "\n") %>%
  separate(sizeAndRecords, into = c("size", "records"), sep = " / ") %>%
  mutate(duration = duration %>% map(parseTime)) %>%
  mutate(size = size %>% map(parseSize)) %>%
  unite("executor", host:executor, sep = ":") %>% 
  mutate(duration = as.numeric(duration), size = as.numeric(size))
  
head(tasks)

p = ggplot(data = tasks, aes(x = factor(cellId), y = duration)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Top longest partitions", y="Time [s]", title="Execution time partition") 
plot(p)
