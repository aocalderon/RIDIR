library(tidyverse)

data0 <- enframe(read_lines("mainus_sdcel_experiment3.txt"), value = "line") |>
  filter(str_detect(line, 'TIME'))

kdtree   <- data0 |> filter(str_detect(line, 'Kdtree')) 
quadtree <- data0 |> filter(str_detect(line, 'Quadtree')) 

data <- kdtree |> bind_rows(quadtree) |>
  separate(col = line, into = c("ts","epoch","appId","tag","partitions","tree","stage","time"), sep = "\\|") |>
  select(partitions, stage, tree, time) |>
  mutate(partitions = as.numeric(partitions), time = as.numeric(time)) |>
  group_by(partitions, tree, stage) |> summarise(time = mean(time)) 

dataTime <- data |> select(partitions, stage, tree, time) |> mutate(partitions = partitions / 1000)

dataCreation <- dataTime |> filter(stage == "creation") |>
  select(partitions, time, tree)

p = ggplot(dataCreation, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested cells (x1000)", y="Creation Tree Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

W = 7
H = 5
ggsave(paste0("K_Creation_US.pdf"), width = W, height = H)

dataPartitioning <- dataTime |> filter(stage == "partitioning") |>
  select(partitions, time, tree)

p = ggplot(dataPartitioning, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested cells (x1000)", y="Partitioning Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 
ggsave(paste0("K_Partitioning_US.pdf"), width = W, height = H)

dataOverlay <- dataTime |> filter(stage == "overlay") |>
  select(partitions, time, tree)

p = ggplot(dataOverlay, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested cells (x1000)", y="Overlay Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 
ggsave(paste0("K_Overlay_US.pdf"), width = W, height = H)

## Space plot
dataSpace <- enframe(read_lines("mainus_sdcel_experiment3.txt"), value = "line") |>
  filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'space')) |>
  separate(col = line, into = c("ts","epoch","appId","tag","partitions","tree","space","nodes"), sep = "\\|") |>
  select(partitions, tree, nodes) |>
  mutate(partitions = as.numeric(partitions), nodes = as.numeric(nodes)) |>
  group_by(partitions, tree) |> summarise(nodes = mean(nodes)) |>
  mutate(partitions = partitions / 1000)

p = ggplot(dataSpace, aes(x = as.factor(partitions), y = nodes, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested cells (x1000)", y="Space (number of nodes)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

ggsave(paste0("K_Space_US.pdf"), width = W, height = H)
