require(tidyverse)

log = read_tsv("log960.tsv")
extract = log %>% filter(phaseName == "Extract DCELs") %>% arrange(duration)
merge   = log %>% filter(phaseName == "Merge DCELs") %>% arrange(duration)

t0 = min(extract$launchTime)

extract0 = extract %>% mutate(t1 = launchTime - t0, t2 = finishTime - t0) %>%
  rowwise() %>% 
  mutate(second = paste(seq(t1, t2), collapse = ' '), taskId = as.factor(index)) %>% 
  separate_rows(second, sep = " ", convert = TRUE) %>%
  select(taskId, host, second)

extract1 = extract0 %>% group_by(host, second) %>% count()

summary(extract1)
print(table(extract1$n))

t0 = min(merge$launchTime)

merge0 = merge %>% mutate(t1 = launchTime - t0, t2 = finishTime - t0) %>%
  rowwise() %>% 
  mutate(Interval = paste(seq(t1, t2), collapse = ' ')) %>% 
  separate_rows(Interval, sep = " ", convert = TRUE) %>%
  select(appId, jobId, stageId, phaseName, host, index, Interval)

merge1 = merge0 %>% group_by(Interval, host) %>% count()

summary(merge1)
print(table(merge1$n))