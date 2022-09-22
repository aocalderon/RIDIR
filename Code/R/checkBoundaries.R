library(tidyverse)

data0 = read_tsv("~/RIDIR/Code/R/boundaries2.tsv", col_names = c('boundaries', 'n'))

data1 = data0 %>% mutate(x = n %% 2 == 0) 

as.list(data1$x) %>% reduce(`&`) ## should be TRUE
