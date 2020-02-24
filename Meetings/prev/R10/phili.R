library(tidyverse)
library(MLmetrics)

data = read_tsv("/home/and/RIDIR/Meetings/R10/phili.tsv", col_names = c('fid', 'area1', 'diff', 'area2'))
show(data)

p = ggplot(data, aes(x = area1, y = area2)) + geom_point() + xlab("Area Source") + ylab("Area DCEL")
plot(p)
R2_Score(data$area1, data$area2)
RMSE(data$area1, data$area2)