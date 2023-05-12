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
  mutate(time = as.numeric(time) / 1000.0) %>%
  mutate(partitions = fct_relevel(partitions, "1000", "2000", "3000", "4000", "5000", "6000", "7000", "8000", "9000", "10000", "11000", "12000", "13000", "14000", "15000")) %>%
  add_column(size = "MainUS")

data3 = data2 %>%
  group_by(partitions, stage, size) %>% summarise(time = mean(time)) %>%
  mutate(partitions = as.factor(partitions)) %>%
  mutate(partitions = recode(partitions,
                             "1000"  = "1K",
                             "2000"  = "2K",
                             "3000"  = "3K",
                             "4000"  = "4K",
                             "5000" = "5K",
                             "6000" = "6K",
                             "7000" = "7K",
                             "8000" = "8K",
                             "9000" = "9K",
                             "10000" = "10K",
                             "11000" = "11K",
                             "12000" = "12K",
                             "13000" = "13K",
                             "14000" = "14K",
                             "15000" = "15K"))

write_tsv(data3, "MainUS.tsv")

p = ggplot(data3, aes(x = partitions, y = time, fill = stage)) + 
  geom_col(width = 0.7) + 
  scale_fill_discrete(labels=c('Layer A', 'Layer B', 'Overlay')) +
  labs(x="Number of cells", y="Time [s]") +
  guides(fill=guide_legend(title="Stages"))
plot(p)

W = as.numeric(Sys.getenv("R_WIDTH"))
H = as.numeric(Sys.getenv("R_HEIGHT"))
ggsave(paste0("MainUS2.pdf"), width = 6, height = 4)