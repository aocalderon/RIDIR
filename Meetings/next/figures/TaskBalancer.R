require(tidyverse)

text = enframe(readLines("output.tsv"))
fields = c("appId", "stageId", "Node", "Time")
data = text %>% separate(col = value, into = fields, sep = "\t") %>%
  mutate(Time = as.numeric(Time))

data0 = data %>% group_by(appId, stageId, Node) %>% summarise(Time = mean(Time))
