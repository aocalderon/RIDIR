require(tidyverse)

sdcel = enframe(readLines("sdcel.txt"))
cgal = enframe(readLines("cgal.txt"))

ops = c("Intersection", "Union", "Symmetric", "DiffA", "DiffB")

sdcel$Op = ops
sdcel$Method = "SDCEL"
cgal$Op = rep(ops, 5)
cgal$Method = "CGAL"

cgal = cgal %>% mutate(Time = as.numeric(value)) %>% select(Method, Op, Time) %>% 
  group_by(Method, Op) %>% summarise(Time = mean(Time))
sdcel = sdcel %>% mutate(Time = as.numeric(value)) %>% select(Method, Op, Time) 

data = sdcel %>% bind_rows(cgal)
data$Op = factor(data$Op, levels = ops)

p = ggplot(data = data, aes(x = Op, y = Time, fill = Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Overlay Operatios", y="Time [s]", title="Execution time by overlay operation") 
plot(p)
ggsave("ByOperators.pdf", width = 10, height = 7, device = "pdf")