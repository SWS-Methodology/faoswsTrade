
reporters <- c(231, 238) # The USA and Ethiopia

reporters <- c(231, 79) # GErmany


if(is.SWSEnvir()) {
  .ojdbcclasspath <- file.path(Sys.getenv("HOME"), "dstrbs", "ojdbc14.jar")
  valid <- fclhs::gettfvalid(reporter = reporters, year = 2011) %>%
    group_by(year, reporter, flow, fcl) %>%
    summarize_each(funs(sum(., na.rm = T)),
                   quantity,
                   value) %>%
    ungroup()
} else {
  valid <- data("tfvalid", package = "tradeproc", envir = environment()) %>%
    group_by(year, reporter, flow, fcl) %>%
    summarize_each(funs(sum(., na.rm = T)),
                   quantity,
                   value) %>%
    ungroup()
}


## Get Valid from file get from Giorgio on the 23th december
test =


testing <- tradedata %>% filter(year == 2011,
                             reporter %in% reporters) %>%
  select(year, reporter, partner, flow, fcl, quantity = qty, value) %>%
  # mutate(flow = ifelse(flow == "Export", 2, 1)) %>%
  mutate_each(funs(as.numeric))  %>%
  group_by(year, reporter, flow, fcl) %>%
  summarize_each(funs(sum(., na.rm = T)),
                      quantity,
                      value) %>%
  ungroup()

joined <- valid %>%
  full_join(testing %>%
              rename(quantity1 = quantity,
                     value1 = value),
            by = c("year", "reporter", "flow", "fcl")) %>%
  mutate(value = value - value1,
         quantity = quantity - quantity1,
         valueprop = value / value1,
         quantityprop = quantity / quantity1) %>%
  select(-ends_with("1")) %>%
  left_join(fclhs::fcl %>%
              select(fcl, fcldesc = fcltitle),
            by = "fcl") %>%
  mutate(reporter = ifelse(reporter == 231, "US", ifelse(reporter == 238, "Ethiopia", reporter)),
         quantityabs = abs(quantity)) %>%
  select(year, reporter, flow, fcl, fcldesc, quantity, quantityabs,
         quantityprop, value, valueprop) %>%
  mutate_each(funs(round(., 0)),
              quantity, quantityabs, value)

XLConnect::writeWorksheetToFile("vignettes/usa_ger.xlsx",
                                as.data.frame(joined),
                                sheet = "comparison")

joined %>% sample_n(100) %>%
ggplot(aes(abs(quantityprop), as.factor(fcl), color = reporter)) +
  geom_point() +
  scale_x_continuous(labels = scales::percent)
