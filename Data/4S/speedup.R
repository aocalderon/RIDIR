library(tidyverse)

data0 = enframe(read_lines("speedup.txt"), value="line")

data1 = data0 %>%
  filter(str_detect(line, 'TIME'))

fields = c("ts","start","appId","time","tag","stage","nodes")
data2 = data1 %>% 
  separate(sep = "\\|", col = "line", into = fields, extra = "drop") %>%
  filter(stage == "end") %>%
  select(time, nodes) %>%
  mutate(time = as.numeric(time))

data3 = data2 %>%
  group_by(nodes) %>% summarise(time = mean(time)) %>%
  mutate(nodes = fct_relevel(nodes, "3", "6", "9", "12"))

p = ggplot(data3, aes(x = nodes, y = time)) + 
  geom_col(width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of nodes", y="Time [s]", title=paste0("Speedup for SDCEL computation"))
plot(p)

ggsave(paste0("speedup.pdf"), width = 8, height = 5)
