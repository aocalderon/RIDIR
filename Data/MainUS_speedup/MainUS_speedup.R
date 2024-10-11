library(tidyverse)
setwd("~/RIDIR/Data/MainUS_speedup/")

data0 = enframe(read_lines("MainUS_speedup_v01.txt"), value="line")

data1 = data0 %>%
  filter(str_detect(line, 'TIME')) 

fields1 = c("ts","start","appId","time","tag","stage","data")
fields2 = c("dataset", "tolerance", "overlay_method", "overlay_level", "partitions", "nodes", "run")
data2 = data1 %>% 
  separate(sep = "\\|", col = "line", into = fields1, extra = "drop") %>%
  separate(sep = "_"  , col = "data", into = fields2, extra = "drop") %>%
  filter(stage == "layer1" | stage == "layer2" | stage == "overlay") %>%
  select(time, stage, nodes) %>%
  mutate(time = as.numeric(time) / 1000.0, nodes = as.factor(nodes))

nodes_labels = c("3", "6", "9", "12")
data3 = data2 %>%
  group_by(nodes, stage) %>% summarise(time = mean(time))  %>%
  mutate(nodes = fct_relevel(nodes, nodes_labels))

write_tsv(data3, "MainUS_speedup.tsv")

stage.labs <- c("Layer A", "Layer B", "Overlay")
names(stage.labs) <- c("layer1", "layer2","overlay")

data4 = data3 |> filter(nodes!="9")

p = ggplot(data4, aes(x = nodes, y = time)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of nodes", y="Time [s]") +
  facet_wrap(~ stage, labeller = labeller(stage = stage.labs))
plot(p)

W = 7
H = 5
ggsave(paste0("MainUS_speedup.pdf"), width = W, height = H)