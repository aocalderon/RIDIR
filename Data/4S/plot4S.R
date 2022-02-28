library(tidyverse)

data1 = read.csv("/home/and/RIDIR/Data/4S/data1.txt")
data2 = read.csv("/home/and/RIDIR/Data/4S/data2.txt")

data3 = bind_rows(data1, data2) %>% filter(Part > 100) %>%
  mutate(Part = as.character(Part)) %>% 
  mutate(Part = fct_relevel(Part, "250", "500", "750", "1000", "2000", "3000", "4000", "5000"))

p = ggplot(data3, aes(x = Part, y = Time)) + 
  geom_col(width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions", y="Time [s]", title=paste0("Performance during SDCEL computation"))
plot(p)
