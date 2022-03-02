library(tidyverse)

data0 = enframe(read_lines("scaleup.txt"), value="line")

data1 = data0 %>%
  filter(str_detect(line, 'TIME'))

fields = c("ts","start","appId","time","tag","stage","nodes","dataset")
data2 = data1 %>% 
  separate(sep = "\\|", col = "line", into = fields, extra = "drop") %>%
  filter(stage == "end") %>%
  select(time, nodes, dataset) %>%
  mutate(time = as.numeric(time))

data3 = data2 %>%
  group_by(dataset) %>% summarise(time = mean(time)) %>%
  mutate(dataset = fct_relevel(dataset, "1", "2", "3", "4"))

p = ggplot(data3, aes(x = dataset, y = time)) + 
  geom_col(width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Size of dataset", y="Time [s]", title=paste0("Scaleup for SDCEL computation"))
plot(p)

ggsave(paste0("scaleup.pdf"), width = 8, height = 5)
