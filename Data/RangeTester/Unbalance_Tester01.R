library(tidyverse)
data0 = enframe(read_lines("Unbalance_Tester01.txt"), value="line")

data1 = data0 %>% filter(str_detect(line, 'INFO')) %>% filter(str_detect(line, 'END')) 

fields = c("tag","ts","runId","size1","size2","method","stage","time")
data2 = data1 %>% 
  separate(sep = "\\t", col = "line", into = fields, extra = "drop") %>%
  select(method, size2, time) %>%
  mutate(time = as.numeric(time) / 1000.0) %>%
  mutate(method = str_replace(method, "Sweeping", "Filter by sweep"))
  
data3 = data2 %>% group_by(method, size2) %>% summarise(time = mean(time)) %>%
        mutate(method = as.factor(method), size = as.factor(size2)) %>%
        mutate(size = recode(size,
                            "6K"  = "2x",
                            "9K"  = "3x",
                            "12K" = "4x",
                            "15K" = "5x",
                            "18K" = "6x",
                            "21K" = "7x")) 
size_order = factor(data3$size, level = c('2x','3x','4x','5x','6x','7x'))

p = ggplot(data3, aes(x = size_order, y = time, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  theme(axis.text.x = element_text(angle = 0, hjust = 0.5, vjust = 0.5)) +
  labs(x="Dataset Size", y="Time [s]") + 
  guides(fill=guide_legend(title="Method"))
plot(p)

W = as.numeric(Sys.getenv("R_WIDTH"))
H = as.numeric(Sys.getenv("R_HEIGHT"))
ggsave(paste0("Unbalance_Tester01.pdf"), width = W, height = H)