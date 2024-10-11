library(tidyverse)

read_tsv("mainus_creation_times.tsv") |>
  pivot_wider(names_from = tree, values_from = time) |>
  mutate(perc = Quadtree * 100 / Kdtree) |> summary()

read_tsv("gadm_creation_times.tsv") |>
  pivot_wider(names_from = tree, values_from = time) |>
  mutate(perc = Quadtree * 100 / Kdtree) |> summary()


mainus_creation <- read_tsv("mainus_creation_times.tsv") |> mutate(stage = "C")
mainus_partition <- read_tsv("mainus_partitioning_times.tsv") |> mutate(stage = "P")
mainus_overlay <- read_tsv("mainus_overlay_times.tsv")  |> mutate(stage = "O")
mainus_kdtree <- mainus_creation |> 
  bind_rows(mainus_partition) |>
  bind_rows(mainus_overlay) |>
  filter(tree == "Kdtree") |>  
  select(partitions, time, stage)
mainus_kdtree |> pivot_wider(names_from = stage, values_from = time) |>
  mutate(T = C + P + O) |>
  mutate(perc = C * 100 / T) |> summary()

