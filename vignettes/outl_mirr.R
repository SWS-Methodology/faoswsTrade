out_coef <- 1.5

# Sefl-trade, reimport, reexport

# Missing quantities and values

tradedata <- tradedata %>%
  mutate(no_quant = qty == 0 | is.na(qty),  # There are no NA qty, but may be changes later
         no_value = value == 0 | is.na(value))


# UV calculation

tradedata <- mutate(tradedata,
                 uv = ifelse(no_quant | no_value, # Only 0 here. Should we care about NA?
                             NA,
                             value / qty))

# Outlier detection

tradedata <- tradedata %>%
  group_by(year, reporter, flow, fcl) %>%
  mutate(
    uv_reporter = median(uv, na.rm = T),
    outlier = uv %in% boxplot.stats(uv, coef = out_coef, do.conf = F)) %>%
  ungroup()

# Imputation of missings and outliers

tradedata <- tradedata %>%
  mutate(qty = ifelse(no_quant | outlier,
                      value / uv_reporter,
                      qty))


# Non reporting countries

data("FAOcountryProfile",
     package = "FAOSTAT",
     envir = environment())

nonreporting <- unique(tradedata$partner)[!is.element(unique(tradedata$partner),
                                                   unique(tradedata$reporter))]

FAOcountryProfile %>%
  select(area = FAOST_CODE,
         name = OFFICIAL_FAO_NAME) %>%
  inner_join(data.frame(area = nonreporting),
             by = "area") %>%
  arrange(name)

tradedatanonrep <- tradedata %>%
  filter(partner %in% nonreporting) %>%
  mutate(partner_mirr = reporter,
         reporter = partner,
         flow = ifelse(flow == 2, 1,
                       ifelse(flow == 1, 2,
                              NA)))

tradedata <- bind_rows(tradedata, tradedatanonrep)
