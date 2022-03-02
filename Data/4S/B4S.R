library(tidyverse)

a0 = enframe(read_lines("/home/and/RIDIR/Data/4S/nohup100.txt"), value="line")
a1 = enframe(read_lines("/home/and/RIDIR/Data/4S/nohup250.txt"), value="line")
a2 = enframe(read_lines("/home/and/RIDIR/Data/4S/nohup500.txt"), value="line")
a3 = enframe(read_lines("/home/and/RIDIR/Data/4S/nohup750.txt"), value="line")

data = bind_rows(a0,a1,a2,a3)

data0 = data %>%
  filter(str_detect(line, 'read|edgesFE'))

stages = rep(c("Start","End"), times = 20)
parts = rep(c(100, 250, 500, 750), each = 10, times = 1)
runs = rep(c(1:5), each = 2, times = 4)

data1 = data0 %>%
  separate(sep = "\\|", col = "line", into = c(NA,"Time",NA), extra = "drop") %>% 
  select(Time) %>% mutate(Time = as.numeric(Time)) %>%
  add_column(Stage = stages, Part = parts, Runs = runs)

data2 = data1 %>%
  pivot_wider(names_from = Stage, values_from = Time)

data3 = data2 %>%
  mutate(Time = (End - Start)/1000.0) %>%
  select(Part, Time) %>%
  group_by(Part) %>%
  summarise(Time = mean(Time))

write_csv(data3, "/home/and/RIDIR/Data/4S/data2.txt")