library(tidyverse)
setwd("~/RIDIR/Data/4S")

data0 = enframe(read_lines("perf_ParVsSeq.txt"), value="line")

data1 = data0 %>%
  filter(str_detect(line, 'TIME')) 

fields = c("ts","start","appId","time","tag","stage","partitions")
data2 = data1 %>% 
  separate(sep = "\\|", col = "line", into = fields, extra = "drop") %>%
  filter(stage == "layer1P" | stage == "layer2P" | stage == "overlayP" ) %>%
  select(time, stage, appId) %>%
  mutate(time = as.numeric(time) / 1000.0) %>%
  group_by(stage) %>% summarise(time = mean(time)) %>% ungroup() %>%
  select(stage, time) %>%
  mutate(stage = substr(stage,1,nchar(stage)-1), timeP = time) %>%
  add_column(part = "Parallel") %>%
  select(stage, timeP, part)

data3 = read_tsv("S4/perf5M.tsv") %>% 
  filter(partitions == 1000) %>%
  add_column(part = "Total") %>%
  mutate(timeT = time) %>%
  select(stage, timeT, part) 
  
data4 = data2 %>%
  left_join(data3, by = "stage") %>%
  mutate(timeS = abs(timeP - timeT)) %>%
  add_column(part = "Sequential") %>%
  select(stage, timeS, part)


data5 = bind_rows(rename(data2, time = timeP), rename(data3, time = timeT), rename(data4, time = timeS))

p = ggplot(data5, aes(x = stage, y = time, fill = part)) +
  geom_col(width = 0.7, position="dodge") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Stage", y="Time [s]", title=paste0("Parallel vs Sequential"))
plot(p)
ggsave(paste0("ParVsSeq.pdf"), width = 8, height = 5)
