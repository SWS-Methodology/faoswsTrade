## USA 231
## GER 79
## ETH 238

reporters = c(238)

if(is.SWSEnvir()) {
  .ojdbcclasspath <- file.path(Sys.getenv("HOME"), "dstrbs", "ojdbc14.jar")
  valid <- fclhs::gettfvalid(reporter = reporters, year = 2011) %>%
    group_by(year, reporter, flow, fcl) %>%
    summarize_each(funs(sum(., na.rm = T)),
                   quantity,
                   value) %>%
    ungroup()
} else {
  data("tfvalid", package = "tradeproc", envir = environment())
  valid <- tfvalid %>%
    filter_(~year == 2011, ~reporter %in% reporters) %>%
    group_by(year, reporter, flow, fcl) %>%
    summarize_each(funs(sum(., na.rm = T)),
                   quantity,
                   value) %>%
    ungroup()
}

## Just a trick before compiling the package again
#load("data/EURconversionUSD.RData")
## As soon the package is loaded
#data("EURconversionUSD", package = "tradeproc", envir = environment())
#exchangeRate = as.numeric(EURconversionUSD %>% filter(Year == 2011) %>% select(ExchangeRate))

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

## This need to go in the producingSet.R only for EU contries
#if(reporters == 79){
#  testing <- testing %>%
#    mutate(value = value*exchangeRate)
#}

joined <- valid %>%
  full_join(testing %>%
              rename(quantity1 = quantity,
                     value1 = value),
            by = c("year", "reporter", "flow", "fcl")) %>%
  mutate(diff_value = value - value1,
         diff_quantity = quantity - quantity1,
         valueprop = value / value1,
         quantityprop = quantity / quantity1,
         value_old = value,
         value_new = value1,
         quantity_old = quantity,
         quantity_new = quantity1) %>%
  select(-ends_with("1")) %>%
  left_join(fclhs::fcl %>%
              select(fcl, fcldesc = fcltitle),
            by = "fcl") %>%
  mutate(reporter = ifelse(reporter == 231, "US",
                           ifelse(reporter == 238, "Ethiopia",
                                  ifelse(reporter == 79, "Germany", reporter))),
         diff_quantityabs = abs(diff_quantity)) %>%
  select(year, reporter, flow, fcl, fcldesc, quantity_old, quantity_new, diff_quantity,
         diff_quantityabs, quantityprop, value_old, value_new, diff_value, valueprop) %>%
  mutate_each(funs(round(., 0)),
              quantity_old, quantity_new, diff_quantity, diff_quantityabs,
              value_old, value_new, diff_value)

## Old quantity != 0, New quantity == 0
joined %>% filter(quantity_old != 0, quantity_new == 0)

## Differences a lot in quantity, but not zeros
joined %>% filter(abs(quantityprop-1)>0.2, abs(diff_quantity) > 1, quantity_new != 0)

## Differences in value
joined %>% filter(abs(valueprop-1)>0.1, abs(diff_value) > 1)

XLConnect::writeWorksheetToFile("vignettes/eth_new.xlsx",
                                as.data.frame(joined),
                                sheet = "comparison")


#XLConnect::writeWorksheetToFile("vignettes/usa_ger_new.xlsx",
#                                as.data.frame(joined),
#                                sheet = "comparison")

joined %>% sample_n(100) %>%
ggplot(aes(abs(quantityprop), as.factor(fcl), color = reporter)) +
  geom_point() +
  scale_x_continuous(labels = scales::percent)
