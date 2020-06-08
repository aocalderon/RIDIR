require(tidyverse)

log = enframe(readLines("test.txt"))
paramsPattern = "partitions|num-executors|executor-cores"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA, "Nodes", NA, "Cores", NA, "Partitions"), sep = " ") %>%
  #mutate(appId = as.numeric(appId)) %>%
  select(appId, Nodes, Cores, Partitions) 

fields = c("Timestamp","Tag1","appId","Nodes","Cores","Partitions","Stage","Time")
mf = log %>% filter(grepl(value, pattern = "\\|DCELMerger\\|")) %>% 
  separate(value, fields, sep = "\\|") %>%
  mutate(Time = as.numeric(Time)) %>%
  #mutate(appId = as.numeric(appId)) %>%
  select(appId, Stage, Time)

data0 = mf %>% inner_join(spark, by = c("appId"))
nStages = length(unique(data0$Stage))
data0$stageId = rep(1:nStages, dim(data0)[1] / nStages)
stageOrder = data0 %>% select(stageId, Stage) %>% distinct() %>% arrange(stageId) %>% select(Stage) %>% as.list()
data0$Stage = factor(data0$Stage, levels = stageOrder$Stage)

data1 = data0 %>% group_by(Partitions, Stage) %>% summarise(Time = mean(Time))

p = ggplot(data = data1, aes(x = Stage, y = Time, fill = Partitions)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Capacity", y="Time [s]", title="Execution time by capacity value") 
plot(p)
#ggsave("test.pdf", width = 10, height = 7, device = "pdf")
