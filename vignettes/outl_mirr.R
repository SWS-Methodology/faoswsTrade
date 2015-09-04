out_coef <- 1.5

# Sefl-trade, reimport, reexport

# Missing quantities and values

tldata <- tldata %>%
  mutate(no_quant = qty == 0 | is.na(qty),  # There are no NA qty, but may be changes later
         no_value = value == 0 | is.na(value))


# UV calculation

tldata <- mutate(tldata,
                 uv = ifelse(no_quant | no_value, # Only 0 here. Should we care about NA?
                             NA,
                             value / qty))

# Outlier detection

tldata <- tldata %>%
  group_by(year, reporter, flow, fcl) %>%
  mutate(
    uv_reporter = median(uv, na.rm = T),
    outlier = uv %in% boxplot.stats(uv, coef = out_coef, do.conf = F)) %>%
  ungroup()

# Imputation of missings and outliers

tldata <- tldata %>%
  mutate(qty = ifelse(no_quant | outlier,
                      value / uv_reporter,
                      qty))


# Non reporting countries

data("FAOcountryProfile",
     package = "FAOSTAT",
     envir = environment())

nonreporting <- unique(tldata$partner)[!is.element(unique(tldata$partner),
                                                   unique(tldata$reporter))]

FAOcountryProfile %>%
  select(area = FAOST_CODE,
         name = OFFICIAL_FAO_NAME) %>%
  inner_join(data.frame(area = nonreporting),
             by = "area") %>%
  arrange(name)

tldatanonrep <- tldata %>%
  filter(partner %in% nonreporting) %>%
  mutate(partner_mirr = reporter,
         reporter = partner,
         flow = ifelse(flow == "Export", "Import",
                       ifelse(flow == "Import", "Export",
                              NA)))

tldata <- bind_rows(tldata, tldatanonrep)
