out_coef <- 1.5

# Sefl-trade, reimport, reexport

# Missing quantities and values

tradedata <- tradedata %>%
  mutate_(no_quant = ~qty == 0 | is.na(qty),  # There are no NA qty, but may be changes later
         no_value = ~value == 0 | is.na(value))


# UV calculation

tradedata <- mutate_(tradedata,
                 uv = ~ifelse(no_quant | no_value, # Only 0 here. Should we care about NA?
                             NA,
                             value / qty))

# Outlier detection

tradedata <- tradedata %>%
  group_by_(~year, ~reporter, ~flow, ~fcl) %>%
  mutate_(
    uv_reporter = ~median(uv, na.rm = T),
    outlier = ~uv %in% boxplot.stats(uv, coef = out_coef, do.conf = F)) %>%
  ungroup()

# Imputation of missings and outliers

tradedata <- tradedata %>%
  mutate_(qty = ~ifelse(no_quant | outlier,
                      value / uv_reporter,
                      qty))


# Non reporting countries

nonreporting <- unique(tradedata$partner)[!is.element(unique(tradedata$partner),
                                                   unique(tradedata$reporter))]

tradedatanonrep <- tradedata %>%
  filter_(~partner %in% nonreporting) %>%
  mutate_(partner_mirr = ~reporter,
         reporter = ~partner,
         flow = ~ifelse(flow == 2, 1,
                       ifelse(flow == 1, 2,
                              NA)))

tradedata <- bind_rows(tradedata,
                       tradedatanonrep)
