library(tidyverse)
data0 = enframe(read_lines("RangeTester3.txt"), value="line")

data1 = data0 %>% filter(str_detect(line, 'INFO')) %>% filter(str_detect(line, 'END')) 

fields = c("tag","ts","runId","size1","size2","method","stage","time")
data2 = data1 %>% 
  separate(sep = "\\t", col = "line", into = fields, extra = "drop") %>%
  select(method, size2, time) %>%
  mutate(time = as.numeric(time) / 1000.0) %>%
  mutate(method = str_replace(method, "Sweeping", "Filter by sweep"))
  
data3 = data2 %>% group_by(method, size2) %>% summarise(time = mean(time)) 
perc = c("6K"="0.5", "9K"="0.33", "12K"="0.25", "15K"="0.20", "18K"="0.16", "21K"="0.14")
size2_order = factor(data3$size2, level = c('6K','9K','12K','15K','18K','21K'))

p = ggplot(data3, aes(x = size2_order, y = time, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  scale_x_discrete(labels = perc) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Dataset Percentage", y="Time [s]",
       title=paste0("Traditional vs Alternative approach during a biased overlay")) + 
  guides(fill=guide_legend(title="Method"))
plot(p)

W = 6
H = 4
ggsave(paste0("RangeTester3.pdf"), width = W, height = H)