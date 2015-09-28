
reporters <- c(231, 238) # The USA and Ethiopia

.ojdbcclasspath <- file.path(Sys.getenv("HOME"), "dstrbs", "ojdbc14.jar")

valid <- fclhs::gettfvalid(reporter = reporters, year = 2011) %>%
  group_by(year, reporter, flow, fcl) %>%
  summarize_each(funs(sum(., na.rm = T)),
                 quantity,
                 value) %>%
  ungroup()

testing <- tldata %>% filter(year == 2011,
                             reporter %in% reporters) %>%
  select(year, reporter, partner, flow, fcl, quantity = qty, value) %>%
  mutate(flow = ifelse(flow == "Export", 2, 1)) %>%
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
  select(year, reporter, flow, fcl, fcldesc, quantity, value, valueprop, quantityprop) %>%
  mutate(reporter = ifelse(reporter == 231, "US", "Ethiopia"))

XLConnect::writeWorksheetToFile("vignettes/usa_eth.xlsx",
                                as.data.frame(joined),
                                sheet = "us_eth")
