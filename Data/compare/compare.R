library(tidyverse)
setwd("~/RIDIR/Data/compare/")

gadm   = read_tsv("../GADM_compare/GADM_compare.tsv") %>%
  add_column(dataset = "GADM")
mainus = read_tsv("../MainUS_compare/MainUS_compare.tsv") %>%
  add_column(dataset = "MainUS")

data = bind_rows(gadm, mainus)

p = ggplot(data, aes(x = dataset, y = time, fill = l1l2)) + 
  geom_col(width = 0.7, position="dodge") + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Dataset", y="Time [s]",
       title=paste0("Comparisson by size of the layers during overlay operation")) + 
  guides(fill=guide_legend(title="Size relation"))
plot(p)

W = as.numeric(Sys.getenv("R_WIDTH"))
H = as.numeric(Sys.getenv("R_HEIGHT"))
ggsave(paste0("compare.pdf"), width = W, height = H)

#i = 1
#ws = c(4.0, 5.0, 6.0, 7.0, 8.0)
#hs = c(2.5, 3.0, 3.5, 4.0, 4.5)
#while(i <= length(ws)) {
#  ggsave(paste0("compare", i, ".pdf"), width = ws[i], height = hs[i])
#  i = i + 1
#}
