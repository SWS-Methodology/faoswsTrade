## Selftrade. Some analysis for 2011
## Feb 2016

library(ggplot2)
library(dplyr)

## Get name countries for plot
faocountries <- read.table("data-raw/CT.m49_FS.csv",
                        header = TRUE,
                        sep = ",",
                        quote = "\"") %>%
  select(reporter = AreaCode,name = CT_iso3)

## Select selftrade flows
selftrade = tradedata %>%
  filter(reporter == partner)

## SUMMARY BY COUNTRY
selftrade_by_coutry = selftrade %>%
  group_by(reporter) %>%
  transmute(nfcl = n_distinct(fcl),
            all_value = sum(value)) %>%
  unique() %>%
  ungroup()
## Adding names ISO3
selftrade_by_coutry = left_join(selftrade_by_coutry, faocountries)
## Plots
ggplot(data = selftrade_by_coutry, aes(nfcl,all_value, label=reporter)) +
  geom_point() + geom_text(aes(label=reporter),hjust=0, vjust=0) +
  xlab("Number of commodity") + ylab("Sum of value (1000USD)")
ggsave("selftrade_by_country_code.pdf", width = 10, height = 8)

ggplot(data = selftrade_by_coutry, aes(nfcl,all_value, label=name)) +
  geom_point() + geom_text(aes(label=name),hjust=0, vjust=0) +
  xlab("Number of commodity") + ylab("Sum of value (1000USD)")
ggsave("selftrade_by_country_name.pdf", width = 10, height = 8)

## SUMMARY BY COMMODITY
selftrade_by_commodity = selftrade %>%
  group_by(fcl) %>%
  transmute(ncountry = n_distinct(reporter),
            all_value = sum(value)) %>%
  unique() %>%
  ungroup()

# Plots
ggplot(data = selftrade_by_commodity, aes(ncountry,all_value, label=fcl)) +
  geom_point() + geom_text(aes(label=fcl),hjust=0, vjust=0) +
  xlab("Number of countries") + ylab("Sum of value (1000USD)")
ggsave("selftrade_by_commodity_code.pdf", width = 10, height = 8)


