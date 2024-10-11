library(tidyverse)
setwd("~/RIDIR/Data/GADM/")

data0a = enframe(read_lines("GADM_v01.txt"), value="line")
data0b = enframe(read_lines("GADM_v02.txt"), value="line")
data0c = enframe(read_lines("GADM_v03.txt"), value="line")

data0 = bind_rows(data0a, data0b, data0c)

data1 = data0 %>%
  filter(str_detect(line, 'TIME')) 

fields1 = c("ts","start","appId","time","tag","stage","data")
fields2 = c("partitions", "dataset", "tolerance", "run")
data2 = data1 %>% 
  separate(sep = "\\|", col = "line", into = fields1, extra = "drop") %>%
  separate(sep = "_"  , col = "data", into = fields2, extra = "drop") %>%
  filter(stage == "layer1" | stage == "layer2" | stage == "overlay") %>%
  select(time, stage, partitions, appId) %>%
  mutate(time = as.numeric(time) / 1000.0) %>%
  mutate(partitions = fct_relevel(partitions, 
                                  #"2000", "4000", "6000", "8000", "10000", "12000", "14000", "16000", "18000", "20000"
                                  as.character(seq(2000, 26000, 2000))
                                  )) %>%
  add_column(size = "gadm")

data3 = data2 %>%
  group_by(partitions, stage, size) %>% summarise(time = mean(time)) %>%
  filter(partitions != 1000 & partitions != 3000 & 
           partitions != 5000 & partitions != 7000 & 
           partitions != 9000 & partitions != 28000 &
           partitions != 22000 & partitions != 24000 &
           partitions != 26000 & partitions != 30000
         ) %>%
  mutate(partitions = as.factor(partitions)) %>%
  mutate(partitions = recode(partitions,
         "2000"  = "16K",
         "4000"  = "18K",
         "6000"  = "20K",
         "8000"  = "22K",
         "10000" = "24K",
         "12000" = "26K",
         "14000" = "28K",
         "16000" = "30K",
         "18000" = "32K",
         "20000" = "34K"#,
         #"22000" = "22K",
         #"24000" = "24K",
         #"26000" = "26K"
         ))

write_tsv(data3, "gadm.tsv")

p = ggplot(data3, aes(x = partitions, y = time, fill = stage)) + 
  geom_col(width = 0.7) + 
  scale_fill_discrete(labels=c('Layer A', 'Layer B', 'Overlay')) +
  labs(x="Number of cells", y="Time [s]") +
  theme(legend.position="bottom", axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(fill=guide_legend(title="Stages"))
plot(p)

W = 5
H = 4
ggsave(paste0("GADM.pdf"), width = W, height = H)