library(tidyverse)

ca <- read_tsv("ca.tsv", col_names = F)
sca <- summary(ca)

mainus <- read_tsv("mainus.tsv", col_names = F)
summary(mainus)
gadm <- read_tsv("gadm.tsv", col_names = F)
summary(gadm)
0