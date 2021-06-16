source("TaskTableReader.R")

apps = read_tsv("apps.tsv", col_names = c("appId", "label")) 

appIds = apps$appId
stageId = 10

#metricList <- list()
#for (i in 1:length(appIds)) {
#  appId = paste0("application_1615435002078_0", appIds[i])
#  print(appId)
#  metricList[[i]] = getTaskSummaryTable(appId, stageId)
#}
#temp = bind_rows(metricList)
# temp %>% write_tsv("temp.tsv")
temp = read_tsv("temp.tsv")

temp1 = apps %>% mutate(appId = paste0("application_1615435002078_0", appId)) %>% inner_join(temp, by = c("appId"))
metric_code = c("Duration"="duration","Scheduler Delay"="scheduler","Task Deserialization Time"="deserial","GC Time"="gc","Result Serialization Time"="serial","Getting Result Time"="result","Peak Execution Memory"="peak","Input Size / Records"="records")
temp1$metric = recode(temp1$metric, !!!metric_code)
data = temp1 %>% filter(metric == "duration" | metric == "records") %>% 
  pivot_wider(names_from = metric, values_from = c(mean, max)) %>%
  select(label, mean_duration, mean_records, max_duration, max_records) %>%
  separate(mean_records, into = c("mean_size", "mean_records"), sep = " / ") %>%
  separate(max_records, into = c("max_size", "max_records"), sep = " / ") %>%
  mutate(mean_duration = as.numeric(mean_duration %>% map(parseTime))) %>%
  mutate(max_duration = as.numeric(max_duration %>% map(parseTime))) %>%
  select(label, mean_records, max_records, mean_duration, max_duration)

data1 = data %>% group_by(label, mean_records, max_records) %>% 
  summarise(mean_duration = mean(mean_duration), max_duration = mean(max_duration)) %>%
  mutate(label = paste0(label,"K"), mean_records = as.numeric(mean_records), max_records = as.numeric(max_records)) 
data1$label = factor(data1$label, levels = c("2K","3K","4K","5K","6K","7K","8K","9K","10K","11K","12K","13K","14K","15K"))

g = ggplot() +
  geom_line(data = data1, aes(x = label, y = max_records, group = 1), color="red") +
  geom_point(data = data1, aes(x = label, y = max_records, group = 1), color="red") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Average number of edges", y="Number of records", title="Maximum number of records per partition") 
ggsave("Max.pdf", width = 8, height = 5)

p = ggplot() +
  geom_line(data = data1, aes(x = label, y = max_duration, group = 1), color="red") +
  geom_point(data = data1, aes(x = label, y = max_duration, group = 1), color="red") +
  geom_line(data = data1, aes(x = label, y = mean_duration, group = 1)) +
  geom_point(data = data1, aes(x = label, y = mean_duration, group = 1)) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Average number of edges", y="Time [s]", title="Execution time per partition") 
ggsave("Run.pdf", width = 8, height = 5)
plot(p)
