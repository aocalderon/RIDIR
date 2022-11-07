library(tidyverse)

data0 = read_tsv("nedgesG_8K.tsv", col_names = c("pid", "sa", "sb")) %>%
  mutate(m = map2_dbl(sa, sb, max)) %>%
  mutate(n = map2_dbl(sa, sb, min)) %>%
  mutate(perc = abs(sa - sb) / m) %>% 
  mutate(p = as.integer(perc * 100)) %>%
  mutate(pid = str_pad(pid,width = 5, pad = "0")) %>%
  arrange(perc)

N = 10
SIZE = 3000
GAP1 = 100
GAP2 = 1
dat = bind_rows(
data0 %>% filter(SIZE - GAP1 <= n & n <= SIZE + GAP1) %>% filter( 9 <= p & p <= 11) %>% sample_n(N) %>% mutate(p = 10),
data0 %>% filter(SIZE - GAP1 <= n & n <= SIZE + GAP1) %>% filter(19 <= p & p <= 21) %>% sample_n(N) %>% mutate(p = 20),
data0 %>% filter(SIZE - GAP1 <= n & n <= SIZE + GAP1) %>% filter(29 <= p & p <= 31) %>% sample_n(N) %>% mutate(p = 30),
data0 %>% filter(SIZE - GAP1 <= n & n <= SIZE + GAP1) %>% filter(39 <= p & p <= 41) %>% sample_n(N) %>% mutate(p = 40),
data0 %>% filter(SIZE - GAP1 <= n & n <= SIZE + GAP1) %>% filter(49 <= p & p <= 51) %>% sample_n(N) %>% mutate(p = 50),
data0 %>% filter(SIZE - GAP1 <= n & n <= SIZE + GAP1) %>% filter(59 <= p & p <= 61) %>% sample_n(N) %>% mutate(p = 60),
data0 %>% filter(SIZE - GAP1 <= n & n <= SIZE + GAP1) %>% filter(69 <= p & p <= 71) %>% sample_n(N) %>% mutate(p = 70)
) %>% select(p, pid)

write_delim(dat, file = "pids3K.txt", col_names = F, delim = ";")