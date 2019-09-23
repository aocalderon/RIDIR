library(tidyverse)

cali = read_tsv("/home/and/Datasets/cali.wkt", col_names = c("geoid", "wkt", "pop")) 
types =  cali %>% separate(wkt, into = c("Type", NA)) %>% group_by(Type) %>% tally()
show(types)
cali %>% filter(str_detect(wkt, "MULTI")) %>% write_tsv("/tmp/cali_multipolygons.wkt")