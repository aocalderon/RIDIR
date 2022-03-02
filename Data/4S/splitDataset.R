library(tidyverse)

wd = "/home/and/Datasets/4S/"
input_file = "SB.wkt"
output_tag = "SB"
parts = 4

polys = read_tsv(paste0(wd, input_file), col_names = c("wkt","id","d","n")) %>%
  mutate(c = cumsum(n))

n = max(polys$c)
q = n / parts

for(i in 1:parts){
  sample = polys %>% filter(c <= i * q) %>% select(wkt)
  write_tsv(sample, paste0("", wd, output_tag, i, ".wkt"), col_names = NA)
}


