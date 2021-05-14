source("JSONReader.R")

apps = enframe(readLines("tasks.txt")) %>%
  filter(grepl(value, pattern = "SparkSubmit --master")) %>%
  separate(value, into = c(NA,NA,"appId",NA), sep = "\\|") 
appIds = pull(apps, appId)

stageMergeId = 10
tasklist = list()
for(i in 1:length(appIds)){
  appId = appIds[i]
  print(appId)
  tasklist[[i]] = getTaskList(appId, stageMergeId, 10000)
}
tasks = bind_rows(tasklist)
tasks %>% write_tsv("tasks.tsv")
