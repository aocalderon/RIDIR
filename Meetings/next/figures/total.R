library(tidyverse)

partitioning = read_tsv("partitioning.tsv")
ldcels = read_tsv("ldcels.tsv")
merge = read_tsv("merge.tsv")

data = bind_rows(partitioning, ldcels, merge) %>%
  mutate(
    tag = fct_relevel(tag, paste0("E", 2:15, "K")),
    stage = fct_relevel(stage, c("sample","quadtree","shuffle","ldcelA","ldcelB","merge"))
  )

p = ggplot(data = data, aes(x = tag, y = time, fill = stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Edges per partition", y="Time [s]", title=paste0("Performance by stages"))
plot(p)

ggsave(paste0("total_by_stages.pdf"), width = 8, height = 5)

total = data %>% group_by(tag) %>% summarise(time = sum(time))

g = ggplot(data = total, aes(x = tag, y = time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Edges per partition", y="Time [s]", title=paste0("Performance"))
plot(g)

ggsave(paste0("total.pdf"), width = 8, height = 5)