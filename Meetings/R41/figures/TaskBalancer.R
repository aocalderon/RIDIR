require(tidyverse)
require(container)

log = enframe(readLines("DCELMerger_CA_04.txt"))
paramsPattern = "partitions"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA, "Partitions"), sep = " ") %>%
  select(appId, Partitions) #%>%
  #mutate(Partitions = as.numeric(Partitions))

stages <- Dict$new(c("17"="Partitioning", "21"="Computing DCEL's", "49"="Merging DCEL's", "57"="Intersection"))

sid = 57
text = enframe(readLines(paste0("outputStage",sid,".tsv")))
fields = c("id", "stageId", "Node", "Time")
executors = text %>% separate(col = value, into = fields, sep = "\t") %>%
  separate(id, into = c(NA, NA, "appId"), sep = "_") %>%
  mutate(Time = as.numeric(Time)) %>%
  select(appId, stageId, Node, Time)

data = spark %>% inner_join(executors, by = c("appId"))
partitionsOrder = data %>% select(Partitions) %>% distinct() %>% mutate(nPartitions = as.numeric(Partitions)) %>% 
  arrange(nPartitions) %>% select(Partitions) %>% as.list()
data$Partitions = factor(data$Partitions, levels = partitionsOrder$Partitions)

data0 = data %>% group_by(Partitions, stageId, Node) %>% summarise(Time = mean(Time))

p = ggplot(data = data0, aes(x = Node, y = Time, group = Partitions)) +
  geom_line(aes(linetype=Partitions, color= Partitions))+
  geom_point(aes(color=Partitions)) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Nodes", y="Time [s]", title=paste0("Task time by Node stage ",stages$peek(paste0(sid)),"[stageId= ", sid, "]")) 
plot(p)

ggsave(paste0("S",sid,"_",stages$peek(paste0(sid)),".pdf"), width = 10, height = 7, device = "pdf")
