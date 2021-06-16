library(tidyverse)
library(ggplot2)

paramsPattern = "qtag"
getParams <- function(command){
  params = str_squish(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}
parseInput <- function(input){
  arr = str_split(input, "/")[[1]]
  return(arr[3])
}

fieldsCommand = c("stamp", "duration", "appId", "command")
params = enframe(readLines("merge.txt")) %>%
  filter(grepl("SparkSubmit", value)) %>%
  separate(value, into = fieldsCommand, sep = "\\|") %>%
  mutate(params = map(command, getParams)) %>%
  separate(params, into = c(NA,"tag"), sep = " ") %>%
  select(appId, tag)

fieldsTime = c("stamp", "duration", "appId", "stage")
stages = enframe(readLines("merge.txt")) %>%
  filter(grepl("Getting LDCELs for B|Merging DCELs", value)) %>%
  separate(value, into = fieldsTime, sep = "\\|") %>%
  mutate(
    stage = recode(stage, "Getting LDCELs for B... done!" = "ldcel", "Merging DCELs... done!" = "merge"),
    duration = as.numeric(duration) / 1000.0
  ) %>%
  select(appId, stage, duration) 

data = stages %>% inner_join(params) %>%
  pivot_wider(names_from = stage, values_from = duration) %>%
  mutate(
    A_merge = merge - ldcel ) %>%
  pivot_longer(cols = starts_with("A_"), names_to = "stage", names_prefix = "A_", values_to = "time") %>%
  select(tag, stage, time) %>%
  group_by(tag, stage) %>% summarise(time = mean(time)) %>% ungroup() %>%
  mutate(
    tag = fct_relevel(tag, c("E2K","E3K","E4K","E5K","E6K","E7K","E8K","E9K","E10K","E11K","E12K","E13K","E14K","E15K"))
    ) 

p = ggplot(data = data, aes(x = tag, y = time, fill = stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Edges per partition", y="Time [s]", title=paste0("Performance during merge"))
plot(p)

ggsave(paste0("merge.pdf"), width = 8, height = 5)
data %>% write_tsv("merge.tsv")

