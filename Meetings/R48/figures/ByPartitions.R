require(tidyverse)

log = enframe(readLines("nohup.txt"))
paramsPattern = "partition"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Partitions"), sep = " ") %>%
  select(appId, Partitions)

fields = c("Timestamp", "Tag", "appId", "Executors", "Cores", "Partitions", "Phase", "Time")
dcel = log %>% filter(grepl(value, pattern = "\\|DCELMerger\\|")) %>% 
  separate(value, fields, sep = "\\|") %>%
  mutate(Phase = str_trim(Phase)) %>%
  mutate(Time = as.numeric(Time)) %>%
  select(appId, Phase, Time)

phaseOrder = c("Reading polygons A",
               "Reading polygons B",
               "Partitioning edges",
               "Extracting A and B DCELs",
               "Updating empty cells",
               "Merging DCELs"
               )
data = dcel %>% inner_join(spark, by = c("appId")) %>%
  group_by(Partitions, Phase) %>% summarise(Time = mean(Time)) %>%
  mutate(Partitions = factor(Partitions, levels = c("4000","6000","8000","10000", "12000"))) %>%
  mutate(Phase = factor(Phase, phaseOrder)) %>%
  mutate(Time = as.numeric(Time))

p = ggplot(data = data, aes(x = Phase, y = Time, fill = Partitions)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Partitions", y="Time [s]", title="Execution time by number of partitions") 
plot(p)

ggsave(paste0("nohup2.pdf"), width = 12, height = 8, device = "pdf")





