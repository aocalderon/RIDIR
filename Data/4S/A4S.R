library(tidyverse)

dataa = enframe(read_lines("/home/and/RIDIR/Data/4S/nohup1.txt"), value="line")
datab = enframe(read_lines("/home/and/RIDIR/Data/4S/nohup2.txt"), value="line")

data = bind_rows(dataa, datab)

data0 = data %>%
  filter(str_detect(line, 'read|edgesFE')) 

stages = rep(c("Start","End"), times = 50)
parts = rep(c(1000,2000,3000,4000,5000), each = 2, times = 10)
runs = rep(c(1:10), each = 10)

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

write_csv(data3, "/home/and/RIDIR/Data/4S/data1.txt")
