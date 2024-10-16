library(tidyverse)

data0 <- enframe(read_lines("gadm_sdcel_experiment1.txt"), value = "line") |>
  filter(str_detect(line, 'TIME'))

kdtree   <- data0 |> filter(str_detect(line, 'Kdtree')) 
quadtree <- data0 |> filter(str_detect(line, 'Quadtree')) 

data_1 <- kdtree |> bind_rows(quadtree) |>
  separate(col = line, into = c("ts","epoch","appId","tag","partitions","tree","stage","time"), sep = "\\|") |>
  select(partitions, stage, tree, time) |>
  mutate(partitions = as.numeric(partitions) / 1000.0, time = as.numeric(time)) |>
  group_by(partitions, tree, stage) |> summarise(time = mean(time)) 

data0 <- enframe(read_lines("gadm_sdcel_experiment2.txt"), value = "line") |>
  filter(str_detect(line, 'TIME'))

kdtree   <- data0 |> filter(str_detect(line, 'Kdtree')) 
quadtree <- data0 |> filter(str_detect(line, 'Quadtree')) 

data_2 <- kdtree |> bind_rows(quadtree) |>
  separate(col = line, into = c("ts","epoch","appId","tag","tag2","partitions","tree","stage","time"), sep = "\\|") |>
  select(partitions, stage, tree, time) |>
  mutate(partitions = as.numeric(partitions) / 1000.0, time = as.numeric(time)) |>
  group_by(partitions, tree, stage) |> summarise(time = mean(time)) 

data <- data_1 |> bind_rows(data_2) |> filter(partitions < 22)

dataTime <- data |> select(partitions, stage, tree, time) 

dataCreation <- dataTime |> filter(stage == "creation") |>
  select(partitions, time, tree)

dataCreation |> write_tsv("gadm_creation_times.tsv")

p = ggplot(dataCreation, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions (x1000)", y="Creation Tree Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

W = 5
H = 4
#ggsave(paste0("K_Creation_GADM.pdf"), width = W, height = H)

dataPartitioning <- dataTime |> filter(stage == "partitioning") |>
  select(partitions, time, tree)

dataPartitioning |> write_tsv("gadm_partitioning_times.tsv")

p = ggplot(dataPartitioning, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions (x1000)", y="Partitioning Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 
#ggsave(paste0("K_Partitioning_GADM.pdf"), width = W, height = H)

dataOverlay <- dataTime |> filter(stage == "overlay") |>
  select(partitions, time, tree)

dataOverlay <- read_tsv("gadm_overlay.tsv")

p = ggplot(dataOverlay, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions (x1000)", y="Overlay Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 
ggsave(paste0("K_Overlay_GADM.pdf"), width = W, height = H)

## Space plot
dataSpace_1 <- enframe(read_lines("gadm_sdcel_experiment1.txt"), value = "line") |>
  filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'space')) |>
  separate(col = line, into = c("ts","epoch","appId","tag","partitions","tree","space","nodes"), sep = "\\|") |>
  select(partitions, tree, nodes) |>
  mutate(partitions = as.numeric(partitions) / 1000.0, nodes = as.numeric(nodes)) |>
  group_by(partitions, tree) |> summarise(nodes = mean(nodes)) 
dataSpace_2 <- enframe(read_lines("gadm_sdcel_experiment2.txt"), value = "line") |>
  filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'space')) |>
  separate(col = line, into = c("ts","epoch","appId","tag","tag2","partitions","tree","space","nodes"), sep = "\\|") |>
  select(partitions, tree, nodes) |>
  mutate(partitions = as.numeric(partitions) / 1000.0, nodes = as.numeric(nodes)) |>
  group_by(partitions, tree) |> summarise(nodes = mean(nodes)) 

dataSpace <- dataSpace_1 |> bind_rows(dataSpace_2) |> filter(partitions < 22000)

p = ggplot(dataSpace, aes(x = as.factor(partitions), y = nodes, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions (x1000)", y="Space (number of nodes)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

#ggsave(paste0("K_Space_GADM.pdf"), width = W, height = H)
