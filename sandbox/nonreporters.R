library(ggplot2)
data("FAOcountryProfile", package = "FAOSTAT")
no_quant_threshold <- .02

tldata %>%
  mutate(no_quant = missingIndicator(weight, 0) & missingIndicator(qty, 0)) %>%
  group_by(reporter) %>%
  summarize(missing_prop = sum(no_quant) / n()) %>%
  ungroup() %>%
  filter(missing_prop > no_quant_threshold) %>%
  arrange(desc(missing_prop)) %>%
  left_join(FAOcountryProfile %>%
              select(reporter = FAOST_CODE,
                     name = FAO_TABLE_NAME),
            by = "reporter") %>%
  ggplot(aes(missing_prop, reorder(name, missing_prop))) +
  geom_point() +
  scale_x_continuous("Trade flows with missing quantities",
                     labels = scales::percent) +
  scale_y_discrete("") +
  ggtitle("Leaders of quantity non-reporting")
