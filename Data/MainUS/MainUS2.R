library(tidyverse)
setwd("~/RIDIR/Data/MainUS/")

data0a = enframe(read_lines("MainUS_v02.txt"), value="line")
data0b = enframe(read_lines("MainUS_v03.txt"), value="line")

data0 = bind_rows(data0a, data0b)

data1 = data0 %>%
  filter(str_detect(line, 'TIME')) 

fields1 = c("ts","start","appId","time","tag","stage","data")
fields2 = c("partitions", "dataset", "tolerance", "run")
data2 = data1 %>% 
  separate(sep = "\\|", col = "line", into = fields1, extra = "drop") %>%
  separate(sep = "_"  , col = "data", into = fields2, extra = "drop") %>%
  filter(stage == "layer1" | stage == "layer2" | stage == "overlay") %>%
  select(time, stage, partitions, appId) %>%
  mutate(time = as.numeric(time) / 1000.0, partitions = as.numeric(partitions)) %>%
  filter(partitions <= 11000) %>%
  mutate(partitions = as.factor(partitions)) %>%
  mutate(partitions = fct_relevel(partitions, "1000", "2000", "3000", "4000", "5000", "6000", "7000", "8000", "9000", "10000", "11000", "12000", "13000", "14000", "15000")) %>%
  add_column(size = "MainUS")

data3 = data2 %>%
  group_by(partitions, stage, size) %>% summarise(time = mean(time)) %>%
  mutate(partitions = as.factor(partitions)) %>%
  mutate(partitions = recode(partitions,
                             "1000"  = "8K",
                             "2000"  = "9K",
                             "3000"  = "10K",
                             "4000"  = "11K",
                             "5000" = "12K",
                             "6000" = "13K",
                             "7000" = "14K",
                             "8000" = "15K",
                             "9000" = "16K",
                             "10000" = "17K",
                             "11000" = "18K"))

write_tsv(data3, "MainUS.tsv")

p = ggplot(data3, aes(x = partitions, y = time, fill = stage)) + 
  geom_col(width = 0.7) + 
  scale_fill_discrete(labels=c('Layer A', 'Layer B', 'Overlay')) +
  labs(x="Number of cells", y="Time [s]") +
  theme(legend.position="bottom", axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(fill=guide_legend(title="Stages"))
plot(p)

W = 5
H = 4
ggsave(paste0("MainUS.pdf"), width = W, height = H)