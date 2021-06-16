require(tidyverse)
require(rvest)

getTaskSummaryTable <- function(appId, stageId) {
  theurl = paste0("http://localhost:18081/history/", appId, 
                  "/stages/stage/?id=", stageId, 
                  "&attempt=0")
  theurl = url(theurl, "rb")
  webpage <- read_html(theurl)
  close(theurl)
  
  tasks = webpage %>%
    html_nodes("#task-summary-table") %>% 
    html_table(fill = TRUE) %>% .[[1]] %>%
    select(X1, X4, X6) %>%
    mutate(appId = appId, stageId = stageId)
  names(tasks) = c("metric", "mean", "max", "appId", "stageId")
  return(tasks)
}

parseTime <- function(str){
  if(str == ""){
    return(0)
  } 
  arr = str_split(str, " ")
  d = as.numeric(arr[[1]][1])
  if(arr[[1]][2] == "ms"){
    d = d / 1000.0
  } else if(arr[[1]][2] == "min"){
    d = d * 60
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

# appId = "application_1615435002078_0439"
# stageMergeId = 10
# 
# tasks = getTaskTable(appId, stageId) 
# head(tasks)
# p = ggplot(data = tasks, aes(x = factor(cellId), y = duration)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
#   theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
#   labs(x="Top longest partitions", y="Time [s]", title="Execution time partition") 
# plot(p)
