library(tidyverse)

data0 <- enframe(read_lines("kdtree2.txt"), value = "line") |>
  filter(str_detect(line, 'TIME'))

kdtree   <- data0 |> filter(str_detect(line, 'Kdtree')) 
quadtree <- data0 |> filter(str_detect(line, 'Quadtree')) 

data <- kdtree |> bind_rows(quadtree) |>
  separate(col = line, into = c("ts","epoch","appId","tag","tree", "stage","partitions","space", "time"), sep = "\\|") |>
  select(partitions, stage, tree, space, time) |>
  mutate(partitions = as.numeric(partitions), space = as.numeric(space), time = as.numeric(time)) |>
  group_by(partitions, tree, stage) |> summarise(space = mean(space), time = mean(time)) 

dataTime <- data |> select(partitions, stage, tree, time)

dataCreationTime <- dataTime |> filter(stage == "kdtree_creation") |>
  bind_rows(dataTime |> filter(stage == "quadtree_creation")) |>
  select(partitions, time, tree)

p = ggplot(dataCreationTime, aes(x = as.factor(partitions), y = time, group = tree)) +
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
ggsave(paste0("CA2_KdtreeVsQuadtree_Creation_time.pdf"), width = W, height = H)

dataOverlayTime <- dataTime |> filter(stage == "kdtree_overlay") |>
  bind_rows(dataTime |> filter(stage == "quadtree_overlay")) |>
  select(partitions, time, tree)

p = ggplot(dataOverlayTime, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions", y="Overlay Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 
ggsave(paste0("CA2_KdtreeVsQuadtree_Overlay_time.pdf"), width = W, height = H)


## Space plot
kdtreeCreation <- data |> filter(stage == "kdtree_creation") 
quadtreeCreation <- data |> filter(stage == "quadtree_creation")
dataSpace <- kdtreeCreation |> bind_rows(quadtreeCreation) |> select(partitions, tree, space)

p = ggplot(data, aes(x = as.factor(partitions), y = space, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of requested partitions", y="Space (number of nodes)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

ggsave(paste0("CA2_KdtreeVsQuadtree_space.pdf"), width = W, height = H)
