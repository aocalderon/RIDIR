require(tidyverse)

log = enframe(readLines("DCELMerger_CA_04.txt"))
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

data = mf %>% inner_join(spark, by = c("appId")) 

nStages = length(unique(data$Stage))
data$stageId = rep(1:nStages, dim(data)[1] / nStages)
stageOrder = data %>% select(stageId, Stage) %>% distinct() %>% arrange(stageId) %>% select(Stage) %>% as.list()
data$Stage = factor(data$Stage, levels = stageOrder$Stage)

partitionsOrder = data %>% select(Partitions) %>% distinct() %>% mutate(nPartitions = as.numeric(Partitions)) %>% 
  arrange(nPartitions) %>% select(Partitions) %>% as.list()
data$Partitions = factor(data$Partitions, levels = partitionsOrder$Partitions)

data = data %>% select(Stage, Partitions, Time) %>%
  group_by(Stage, Partitions) %>% summarise(Time = mean(Time))

data1 = data %>% group_by(Partitions, Stage) %>% summarise(Time = mean(Time)) %>%
  filter(Stage != "Reading polygons A            ") %>%
  filter(Stage != "Reading polygons B            ")


p = ggplot(data = data1, aes(x = Stage, y = Time, fill = Partitions)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Stages", y="Time [s]", title="Execution time by stages") 
plot(p)
ggsave("ByStages.pdf", width = 10, height = 7, device = "pdf")

data2 = data1 %>% group_by(Partitions) %>% summarise(Time = sum(Time))
p = ggplot(data = data2, aes(x = Partitions, y = Time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Partitions", y="Time [s]", title="Execution time") 
plot(p)
ggsave("ByPartitions.pdf", width = 10, height = 7, device = "pdf")
