library(tidyverse)

data0 <- enframe(read_lines("kdtree_ph.txt"), value = "line") |>
  filter(str_detect(line, 'TIME'))

kdtree   <- data0 |> filter(str_detect(line, 'Kdtree')) 
quadtree <- data0 |> filter(str_detect(line, 'Quadtree')) 

data <- kdtree |> bind_rows(quadtree) |>
  separate(col = line, into = c("ts","epoch","appId","tag","tree","partitions","space", "time"), sep = "\\|") |>
  select(partitions, tree, space, time) |>
  mutate(partitions = as.numeric(partitions), space = as.numeric(space), time = as.numeric(time)) |>
  group_by(partitions, tree) |> summarise(space = mean(space), time = mean(time)) |>
  filter(partitions != 256)

p = ggplot(data, aes(x = as.factor(partitions), y = time, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of partitions", y="Time(s)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

W = 6
H = 4
ggsave(paste0("PH_KdtreeVsQuadtree_time.pdf"), width = W, height = H)

p = ggplot(data, aes(x = as.factor(partitions), y = space, group = tree)) +
  geom_line(aes(linetype = tree, color = tree)) + 
  geom_point(aes(shape = tree, color = tree), size = 3) +
  labs(x="Number of partitions", y="Space (number of leaves)") +
  scale_color_discrete("Tree") +
  scale_shape_discrete("Tree") +
  guides(linetype = "none") +
  theme_bw()
plot(p) 

ggsave(paste0("PH_KdtreeVsQuadtree_space.pdf"), width = W, height = H)