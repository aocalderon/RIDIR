library(tidyverse)

data0 <- enframe(read_lines("gadm_sdcel_experiment1.txt"), value = "line") |>
  filter(str_detect(line, 'TIME'))

kdtree   <- data0 |> filter(str_detect(line, 'Kdtree')) 
quadtree <- data0 |> filter(str_detect(line, 'Quadtree')) 

data_1 <- kdtree |> bind_rows(quadtree) |>
  separate(col = line, into = c("ts","epoch","appId","tag","partitions","tree","stage","time"), sep = "\\|") |>
  select(partitions, stage, tree, time) |>
  mutate(partitions = as.numeric(partitions), time = as.numeric(time)) |>
  group_by(partitions, tree, stage) |> summarise(time = mean(time)) 

data0 <- enframe(read_lines("gadm_sdcel_experiment2.txt"), value = "line") |>
  filter(str_detect(line, 'TIME'))

kdtree   <- data0 |> filter(str_detect(line, 'Kdtree')) 
quadtree <- data0 |> filter(str_detect(line, 'Quadtree')) 

data_2 <- kdtree |> bind_rows(quadtree) |>
  separate(col = line, into = c("ts","epoch","appId","tag","tag2","partitions","tree","stage","time"), sep = "\\|") |>
  select(partitions, stage, tree, time) |>
  mutate(partitions = as.numeric(partitions), time = as.numeric(time)) |>
  group_by(partitions, tree, stage) |> summarise(time = mean(time)) 

data <- data_1 |> bind_rows(data_2) |> filter(partitions < 22000)

dataTime <- data |> select(partitions, stage, tree, time)

dataCreation <- dataTime |> filter(stage == "creation") |>
  select(partitions, time, tree)

p = ggplot(dataCreation, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions", y="Creation Tree Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

W = 8
H = 6
ggsave(paste0("GADM_KdtreeVsQuadtree_Creation.pdf"), width = W, height = H)

dataPartitioning <- dataTime |> filter(stage == "partitioning") |>
  select(partitions, time, tree)

p = ggplot(dataPartitioning, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions", y="Partitioning Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 
ggsave(paste0("GADM_KdtreeVsQuadtree_Partitioning.pdf"), width = W, height = H)

dataOverlay <- dataTime |> filter(stage == "overlay") |>
  select(partitions, time, tree)

#dataOverlay |> write_tsv("gadm_overlay.tsv")

dataOverlay <- read_tsv("gadm_overlay.tsv")

p = ggplot(dataOverlay, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions", y="Overlay Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 
ggsave(paste0("GADM_KdtreeVsQuadtree_Overlay.pdf"), width = W, height = H)

## Space plot
dataSpace_1 <- enframe(read_lines("gadm_sdcel_experiment1.txt"), value = "line") |>
  filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'space')) |>
  separate(col = line, into = c("ts","epoch","appId","tag","partitions","tree","space","nodes"), sep = "\\|") |>
  select(partitions, tree, nodes) |>
  mutate(partitions = as.numeric(partitions), nodes = as.numeric(nodes)) |>
  group_by(partitions, tree) |> summarise(nodes = mean(nodes)) 
dataSpace_2 <- enframe(read_lines("gadm_sdcel_experiment2.txt"), value = "line") |>
  filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'space')) |>
  separate(col = line, into = c("ts","epoch","appId","tag","tag2","partitions","tree","space","nodes"), sep = "\\|") |>
  select(partitions, tree, nodes) |>
  mutate(partitions = as.numeric(partitions), nodes = as.numeric(nodes)) |>
  group_by(partitions, tree) |> summarise(nodes = mean(nodes)) 

dataSpace <- dataSpace_1 |> bind_rows(dataSpace_2) |> filter(partitions < 22000)

p = ggplot(dataSpace, aes(x = as.factor(partitions), y = nodes, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions", y="Space (number of nodes)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

ggsave(paste0("GADM_KdtreeVsQuadtree_Space.pdf"), width = W, height = H)
