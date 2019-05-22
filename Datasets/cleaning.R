library(tidyverse)

p = read_tsv("/home/and/RIDIR/Datasets/phili_2010.wkt", col_names = c("wkt", "tid", "extensive", "intensive"))

head(p)
nrow(p)

p_clean = p %>% filter(intensive != "null")
head(p_clean)
nrow(p_clean)

write_tsv(p_clean, "/home/and/RIDIR/Datasets/phili_2010a.wkt", col_names = FALSE)