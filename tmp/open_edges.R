library(tidyverse)

data <- read_tsv("~/RIDIR/tmp/tx.txt", col_names = F)
parts <- 2410

a1 <- mean(data$X2) * parts
a2 <- mean(data$X3) * parts
print(a1)
print(a2)
print(a2/a1)

data <- read_tsv("~/RIDIR/tmp/va.txt", col_names = F)
parts <- 2116

a1 <- mean(data$X2) * parts
a2 <- mean(data$X3) * parts
print(a1)
print(a2)
print(a2/a1)
