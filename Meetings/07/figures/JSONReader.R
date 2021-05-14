require(tidyverse)
library(httr)
library(jsonlite)
library(rlist)

getTaskList <- function(appId, sId, nTasks){
  theurl = paste0("http://localhost:18081/api/v1/applications/"
                  , appId
                  ,"/stages?status=complete")
  resp = GET(theurl)
  jsonTXT = content(resp, as="text")
  json = fromJSON(jsonTXT)
  attempts = json %>% filter(stageId == sId) %>% select(stageId, status, attemptId)
  attemptId = attempts$attemptId[1]
  
  theurl = paste0("http://localhost:18081/api/v1/applications/", appId, 
                  "/stages/", sId, 
                  "/", attemptId, "/taskList?offset=0&length=", nTasks, "&sortBy=-runtime")
  #print(theurl)
  resp = GET(theurl)
  jsonTXT = content(resp, as="text")
  json = fromJSON(jsonTXT)
  tasks = as_tibble(json) %>% select(taskId, index, host, executorId, duration, launchTime, status)
  tasks$runtime     = json$taskMetrics$executorRunTime
  tasks$bytesRead   = json$taskMetrics$inputMetrics$bytesRead
  tasks$recordsRead = json$taskMetrics$inputMetrics$recordsRead
  tasks$shuffleBytesRead   = json$taskMetrics$shuffleReadMetrics$remoteBytesRead
  tasks$shuffleRecordsRead = json$taskMetrics$shuffleReadMetrics$recordsRead
  
  tasks$appId       = appId
  tasks$stageId     = sId
  tasks$attemptId   = attemptId
  
  return(tasks)
}

getCellInfo <- function(ids){
  cell_list = list()
  for(i in 1:length(ids)){
    id = ids[i]
    filenameA = paste0("cells/cellsA_P", id, "K.tsv")
    filenameB = paste0("cells/cellsB_P", id, "K.tsv")
    A = read_tsv(filenameA, col_names = c("index", "nA", "wkt", "lineage", "id")) %>%
      select(index, nA, wkt)
    B = read_tsv(filenameB, col_names = c("index", "nB", "wkt", "lineage", "id")) %>%
      select(index, nB)
    cells = A %>% inner_join(B, by = c("index"))
    cells$tag = paste0("P",id,"K")
    cell_list[[i]] = cells %>% select(tag, index, nA, nB, wkt)
  }
  cells = bind_rows(cell_list)
  cells %>% write_tsv("cells.tsv")
}