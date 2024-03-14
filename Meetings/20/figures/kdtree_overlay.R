library(tidyverse)

data0 <- enframe(read_lines("kdtree_overlay_TN.txt"), value = "line") |>
  filter(str_detect(line, 'TIME'))

kdtree   <- data0 |> filter(str_detect(line, 'Kdtree')) 
quadtree <- data0 |> filter(str_detect(line, 'Quadtree')) 

data <- kdtree |> 
  #bind_rows(quadtree) |>
  separate(col = line, into = c("ts","epoch","appId","tag","dataset","partitions","tree","stage","time"), sep = "\\|") |>
  select(appId, dataset, stage, tree, time) |>
  mutate(time = as.numeric(time)) 

appIds <- enframe(read_lines("kdtree_overlay_TN.txt"), value = "line") |>
  filter(str_detect(line, 'COMMAND')) |>
  separate(col = line, into = c("ts","epoch","appId","tag","params"), sep = "\\|") |>
  select(appId)

labels = c("By Label", 
                  "Master", 
                  "Level [4]", 
                  "Level [5]", 
                  "Level [6]", 
                  "Level [7]", 
                  "Level [8]", 
                  "Level [9]", 
                  "Level [10]")

labels_prime <- data.frame(method = rep(labels, 5))
overlay_labels <- appIds |> bind_cols(labels_prime)

data1 <- data |> left_join(overlay_labels, by = c("appId")) |>
  filter(stage == "overlay") |>
  select(tree, method, time) |>
  group_by(tree, method) |> summarise(time = mean(time)) |>
  mutate(method = fct_relevel(method, labels))

p = ggplot(data1, aes(x = method, y = time)) + 
  geom_col(width = 0.7, position="dodge") + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5)) +
  labs(x="Method of overlay", y="Time [s]")
plot(p)

W=8
H=6
ggsave(paste0("TN_Overlay_Tester.pdf"), width = W, height = H)
