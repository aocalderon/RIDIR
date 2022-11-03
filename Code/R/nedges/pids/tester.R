library(tidyverse)
data0 = enframe(read_lines("tester1.txt"), value="line")

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
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Dataset Size by percentage", y="Time [s]",
       title=paste0("Traditional vs Alternative approach during a biased overlay")) + 
  guides(fill=guide_legend(title="Method"))
plot(p)

W = 6
H = 4
ggsave(paste0("RangeRealData.pdf"), width = W, height = H)

getDataPerPerc <- function(data, P){
  data20 = data %>% filter(perc == P) %>% select(file, time, method) %>%
    group_by(file, method) %>% summarise(time = mean(time))
  data20a = data20 %>% group_by(method) %>% summarise(time = mean(time))
  
  p = ggplot(data20, aes(x = file, y = time, fill = method)) + 
    geom_col(width = 0.7, position="dodge") +
    labs(x="Cells", y="Time [s]", title=paste0("Cells ",P,"% different by Time")) + 
    guides(fill=guide_legend(title="Layer"))
  plot(p)
  ggsave(paste0("Cell",P,"Time.pdf"), width = W, height = H)
  
  data20 = data %>% filter(perc == P) %>% select(file, n1, n2) %>% distinct(file, n1, n2) %>%
    pivot_longer(c(n1,n2), names_to = "n", values_to = "size") %>% mutate(size = as.numeric(size))
  
  p = ggplot(data20, aes(x = file, y = size, fill = n)) + 
    geom_col(width = 0.7, position="dodge") + 
    labs(x="Cells", y="Size", title=paste0("Cells ",P,"% different by Size")) + 
    guides(fill=guide_legend(title="Layer"))
  plot(p)
  ggsave(paste0("Cell",P,"Size.pdf"), width = W, height = H)
}

data = data1 %>% separate(sep = "\\t", col = "line", into = fields, extra = "drop") %>%
  select(perc, file, method, time, n1, n2) %>% mutate(time = as.numeric(time) / 1000.0)

getDataPerPerc(data, 50)