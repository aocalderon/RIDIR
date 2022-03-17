library(tidyverse)
setwd("~/RIDIR/Data/4S")

s5 = read_tsv("S4/perf5M.tsv")
s3 = read_tsv("S3/perf3M.tsv")
s2 = read_tsv("S2/perf2M.tsv")
s1 = read_tsv("S1/perf1M.tsv")

data0 = bind_rows(s5, s3, s2, s1) %>%
  mutate(partitions = as.factor(partitions))

data1 = data0 %>% filter(stage == "overlay")
p = ggplot(data1, aes(x = partitions, y = time, group = size)) + 
  geom_line(aes(linetype=size)) + 
  geom_point(aes(shape=size)) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions", y="Time [s]", title=paste0("Overlay performance by partitions and size for SDCEL computation"))
plot(p)
ggsave(paste0("overlay.pdf"), width = 8, height = 5)

data1 = data0 %>% filter(stage == "layer2")
p = ggplot(data1, aes(x = partitions, y = time, group = size)) + 
  geom_line(aes(linetype=size)) + 
  geom_point(aes(shape=size)) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions", y="Time [s]", title=paste0("Layer 2 performance by partitions and size for SDCEL computation"))
plot(p)
ggsave(paste0("layer2.pdf"), width = 8, height = 5)

data1 = data0 %>% filter(stage == "layer1")
p = ggplot(data1, aes(x = partitions, y = time, group = size)) + 
  geom_line(aes(linetype=size)) + 
  geom_point(aes(shape=size)) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions", y="Time [s]", title=paste0("Layer 1 performance by partitions and size for SDCEL computation"))
plot(p)
ggsave(paste0("layer1.pdf"), width = 8, height = 5)
