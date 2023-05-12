library(tidyverse)
data0 = enframe(read_lines("Unbalance_Tester02.txt"), value="line")

data1 = data0 %>% filter(str_detect(line, 'INFO')) %>% filter(str_detect(line, 'END')) 

fields = c("tag","ts","runId","perc","file","method","stage","time","p","n1","n2")
data2 = data1 %>% 
  separate(sep = "\\t", col = "line", into = fields, extra = "drop") %>%
  select(method, perc, time) %>%
  mutate(time = as.numeric(time) / 1000.0, perc = as.factor(as.numeric(perc) / 100.0)) %>%
  mutate(method = str_replace(method, "Sweeping", "Filter by sweep"))

data3 = data2 %>% group_by(method, perc) %>% summarise(time = mean(time)) 

p = ggplot(data3, aes(x = perc, y = time, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Percentage difference between datasets", y="Time [s]") + 
  guides(fill=guide_legend(title="Method"))
plot(p)

W = as.numeric(Sys.getenv("R_WIDTH"))
H = as.numeric(Sys.getenv("R_HEIGHT"))
ggsave(paste0("Unbalance_Tester02.pdf"), width = W, height = H)
