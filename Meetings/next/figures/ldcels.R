library(tidyverse)
library(ggplot2)

paramsPattern = "qtag"
getParams <- function(command){
  params = str_squish(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

fieldsCommand = c("stamp", "duration", "appId", "time", "tag", "command")
params = enframe(readLines("ldcels.txt")) %>%
  filter(grepl("COMMAND", value)) %>%
  separate(value, into = fieldsCommand, sep = "\\|") %>%
  mutate(params = map(command, getParams)) %>%
  separate(params, into = c(NA, "tag"), sep = " ") %>%
  unnest(tag) %>%
  select(appId, tag)

fieldsTime = c("stamp", "duration", "appId", "time", "tag", "stage")
stages = enframe(readLines("ldcels.txt")) %>%
  filter(grepl("TIME", value)) %>%
  separate(value, into = fieldsTime, sep = "\\|") %>%
  select(appId, stage, time) %>%
  mutate(time = as.numeric(time) / 1000.0)

data = stages %>% inner_join(params) %>%
  select(tag, stage, time) %>%
  group_by(tag, stage) %>%  summarise(time = mean(time)) %>% ungroup() %>%
  mutate(
    tag = fct_relevel(tag, paste0("E", 2:15, "K"))
  ) %>% filter(grepl("ldcel", stage))

p = ggplot(data = data, aes(x = tag, y = time, fill = stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Edges per partition", y="Time [s]", title=paste0("Performance during local DCEL computation"))
plot(p)

ggsave(paste0("ldcels.pdf"), width = 8, height = 5)
data %>% write_tsv("ldcels.tsv")

