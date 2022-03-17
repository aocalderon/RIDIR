library(tidyverse)
setwd("~/RIDIR/Data/4S/S2")

data0 = enframe(read_lines("perf2M.txt"), value="line")

data1 = data0 %>%
  filter(str_detect(line, 'TIME')) 

fields = c("ts","start","appId","time","tag","stage","partitions")
data2 = data1 %>% 
  separate(sep = "\\|", col = "line", into = fields, extra = "drop") %>%
  filter(stage == "layer1" | stage == "layer2" | stage == "overlay") %>%
  select(time, stage, partitions, appId) %>%
  mutate(time = as.numeric(time) / 1000.0) %>%
  mutate(partitions = fct_relevel(partitions, "100", "250", "500", "750", "1000", "2000", "3000", "4000", "5000", "6000", "7000")) %>%
  add_column(size = "2.5M")

data3 = data2 %>%
  group_by(partitions, stage, size) %>% summarise(time = mean(time))  

write_tsv(data3, "perf2M.tsv")

p = ggplot(data3, aes(x = partitions, y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions", y="Time [s]", title=paste0("Performance census dataset (2.5M edges) for SDCEL computation"))
plot(p)

ggsave(paste0("perf2M.pdf"), width = 8, height = 5)