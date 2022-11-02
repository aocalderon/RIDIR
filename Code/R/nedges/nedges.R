library(tidyverse)

data0 = read_tsv("nedgesG_8K.tsv", col_names = c("pid", "sa", "sb")) %>%
  mutate(m = map2_dbl(sa, sb, max)) %>%
  mutate(n = map2_dbl(sa, sb, min)) %>%
  mutate(perc = abs(sa - sb) / m) %>% 
  mutate(p = as.integer(perc * 100)) %>%
  mutate(pid = str_pad(pid,width = 5, pad = "0")) %>%
  arrange(perc)

limit = 3000
data00 = data0 %>% filter(p ==  0 & m >= limit) %>% sample_n(10)
data10 = data0 %>% filter(p == 10 & m >= limit) %>% sample_n(10) 
data20 = data0 %>% filter(p == 20 & m >= limit) %>% sample_n(10) 
data30 = data0 %>% filter(p == 30 & m >= limit) %>% sample_n(10) 
data40 = data0 %>% filter(p == 40 & m >= limit) %>% sample_n(10) 
data50 = data0 %>% filter(p == 50 & m >= limit) %>% sample_n(10) 
data60 = data0 %>% filter(p == 60 & m >= limit) %>% sample_n(10) 
data70 = data0 %>% filter(p == 70 & m >= limit) %>% sample_n(10) 
data80 = data0 %>% filter(p == 80 & m >= limit) %>% sample_n(10) 
data90 = data0 %>% filter(p == 90 & m >= limit) %>% sample_n(10) 

dat = bind_rows(data00,data10,data20,data30,data40,data50,data60,data70,data80,data90) %>% 
  select(p, pid)

write_delim(dat, file = "pids.txt", col_names = F, delim = ";")