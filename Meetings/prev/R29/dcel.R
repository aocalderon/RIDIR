require(tidyverse)

data = enframe(read_lines("dcel.txt"), value = "line") %>% select(line)

head(data, n = 20)

fields = c("Timestamp", "Tag", "appId", "Cores", "Executors", "Partitions", "Phase", "Time")
data2 = data %>% filter(grepl("Partitioning edges|Extracting segments|Getting half|Getting local", line)) %>%
  separate(line, into = fields, sep = "\\|") %>%
  mutate(Partitions = as.numeric(Partitions), Time = as.numeric(Time), Phase = str_trim(Phase)) 
head(data2, n = 20)

data3 = data2 %>%
  select(appId, Partitions, Time) %>%
  group_by(Partitions, appId) %>% summarise(Time = sum(Time)) %>% ungroup() %>%
  group_by(Partitions) %>% summarise(Time = mean(Time))
head(data3, n = 20)

data4 = data2 %>%
  select(appId, Partitions, Phase, Time) %>%
  #group_by(Partitions, appId, Phase) %>% summarise(Time = sum(Time)) %>% ungroup() %>%
  group_by(Partitions, Phase) %>% summarise(Time = mean(Time)) %>% 
  mutate( Phase = case_when(
    Phase == "Partitioning edges" ~ "1.Partitioning edges",
    Phase == "Extracting segments" ~ "2.Extracting segments",
    Phase == "Getting half edges" ~ "3.Getting half edges",
    Phase == "Getting local DCEL's" ~ "4.Merging DCELs"
  ))
head(data4, n = 20)

p = ggplot(data = data4, aes(x = factor(Partitions), y = Time, group = Phase)) +
  geom_line(aes(linetype = Phase, color = Phase)) +
  geom_point(aes(shape = Phase, color = Phase)) + 
  labs(title="CA Dataset", x="Number of partitions", y="Execution time [s]") +
  theme(legend.position="right")
plot(p)

ggsave("CA_phases.pdf", width = 7, height = 5, device = "pdf")