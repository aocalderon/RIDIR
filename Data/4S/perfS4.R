library(tidyverse)
setwd("~/RIDIR/Data/4S")

s5 = read_tsv("S4/perf5M.tsv")
s3 = read_tsv("S3/perf3M.tsv")
s2 = read_tsv("S2/perf2M.tsv")
s1 = read_tsv("S1/perf1M.tsv")

data0 = bind_rows(s5, s3, s2, s1) %>%
  mutate(partitions = as.factor(partitions))

p = ggplot(data0, aes(x = partitions, y = time, fill = size)) + 
  geom_col(width = 0.7, position="dodge") + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions", y="Time [s]", title=paste0("Total performance by partitions and size for SDCEL computation"))
plot(p)
ggsave(paste0("total.pdf"), width = 8, height = 5)

data1 = data0 %>% filter(stage == "overlay")
p = ggplot(data1, aes(x = partitions, y = time, fill = size)) + 
  geom_col(width = 0.7, position="dodge") + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions", y="Time [s]", title=paste0("Overlay performance by partitions and size for SDCEL computation"))
plot(p)
ggsave(paste0("overlay.pdf"), width = 8, height = 5)

data1 = data0 %>% filter(stage == "layer2")
p = ggplot(data1, aes(x = partitions, y = time, fill = size)) + 
  geom_col(width = 0.7, position="dodge") + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions", y="Time [s]", title=paste0("Layer performance by partitions and size for SDCEL computation"))
plot(p)
ggsave(paste0("layer.pdf"), width = 8, height = 5)
