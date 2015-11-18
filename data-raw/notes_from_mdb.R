library(testthat)
library(dplyr, warn.conflicts = F)
library(stringr)

directory <- file.path("", "mnt", "essdata", "TradeSys", "TradeSys", "Countries")

areas_years <- data.frame(dirnames = dir(directory, full.names = F),
                          stringsAsFactors = F) %>%
  filter(str_detect(dirnames, "^[A-Z]{3}_[1-2][0-9]{3}$")) %>%
  mutate(area = str_extract(dirnames, "^[A-Z]{3}"),
         year = as.integer(str_extract(dirnames, "[1-2][0-9]{3}$")))

areas_years_latest <- areas_years %>%
  group_by(area) %>%
  filter(year == max(year)) %>%
  ungroup()

# Notes table

plyr::ldply(areas_years_latest$dirnames,
            function(x) {
              if(x == "CLA_2002") x <- "ECU_2000"
              if(x == "BYE_2013") x <- "AZE_2013"
              basefilename <- paste0(x, ".mdb")
              filename <- file.path(directory, x, basefilename)
              table <- "Notes"

              data <- try(Hmisc::mdb.get(filename,
                                         colClasses = "character",
                                         stringsAsFactors = F,
                                         lowernames = T,
                                         tables = table,
                                         na.strings = c("", "N.A.")))

              if(inherits(data, "try-error")) {
                warning(paste0(x, " failed"))
                return(data.frame())
              }


              if(nrow(data) > 0) { # Condition added, because in IRQ 2009 dataset is empty
                data$reporter <- str_extract(x, "^[A-Z]{3}")
              }

              data
            },
            .inform = FALSE,
            .progress = "text"

) -> notes

# notes to adjustments ####

adjustments <- notes %>%
  select_(
    ~reporter,
    ~year,
    ~flow,
    hs = ~original.item,
    fcl = ~item,
    partner = ~country,
    ~quantity,
    ~quantity.other,
    value = ~val,
    ~special) %>%
  # 3) GRN 2002 was not used (to be deleted) and so negative data should not be there
  filter_(~!(reporter == "GRN" & year == "2002")) %>%
  # > Should I drop them or make a workaround to deal with ranges?
  # Yes, please drop them!
  filter_(~!str_detect(hs, "^\\d+\\-\\d+$") | is.na(hs)) %>%
  # 4) Denmark 2012 was processed from home and personal laptop;
  # that's why there are commas (not accepted by the system )
  mutate_(quantity = ~ifelse(reporter == "DEN" & year == "2012",
                             stringr::str_replace(quantity, ",", "."),
                             quantity)) %>%
  mutate_(special =  ~ifelse(reporter == "FIN" & year == "2012",
                             stringr::str_replace(special, ",", "."),
                             special)) %>%
  mutate_(special = ~stringr::str_trim(special)) %>%
  # Drop flag M from special
  mutate_(special =  ~ifelse((reporter == "FIN" & year == "2003") |
                               (reporter == "QAT" & year == "2013"),
                             stringr::str_replace(special, "^M", ""),
                             special)) %>%
  # Drop flag F from special
  mutate_(special = ~ifelse(special %in% c("F24000", "F0.9"),
                            stringr::str_replace(special, "^F", ""),
                            special))


### Quantity tests ####

# 1)for "SET" quantities we have the possibility to put F for estimation and M for no symbol
#
# 2) O means Oracle Data (already present in the FWS)
#
# Ciao Claudia

test_that("All nonnumeric variants of quantity are expected", {
  expect_match(unique(adjustments$quantity[grepl("[a-zA-Z]",
                                                 adjustments$quantity)]),
               "QUANTITY_ORIG|QUANTITY_OTHER|VALUE_ORIG|VALUE|ORACLE_DATA|^[M,F,O]\\d*\\.?\\d*$"
               )
})


test_that("All nonnumeric variants of value are expected", {
  expect_match(unique(adjustments$value[grepl("[a-zA-Z]",
                                              adjustments$value)]),
               # Vector of expected values in VALUE
               "VALUE_ORIG|QUANTITY|QUANTITY_OTHER|ORACLE_DATA|^[M,F,O]\\d*\\.?\\d*$")
})


test_that("All nonnumeric variants of quantity.other are expected", {
  expect_match(
    unique(adjustments$quantity.other[grepl("[a-zA-Z]",
                                            adjustments$quantity.other)]),
    "QUANTITY")
})

test_that("Flow values are expected", {
  expect_match(adjustments$flow,
               "^[0-4]$")
})

test_that("All hs codes are numeric", {
  expect_match(str_trim(adjustments$hs[!is.na(adjustments$hs)]),
               "^\\d+$")
})

test_that("All fcl codes are numeric", {
  expect_match(str_trim(adjustments$fcl[!is.na(adjustments$fcl)]),
               "^\\d+$")
})


test_that("All partner codes are numeric", {
  expect_match(str_trim(adjustments$partner[!is.na(adjustments$partner)]),
               "^\\d+$")
})


test_that("All years are numeric", {
  expect_match(str_trim(adjustments$year[!is.na(adjustments$year)]),
               "^\\d+$")
})



adjustments <- adjustments %>%
  mutate_(
    flow = ~as.integer(flow),
    # flow 0 means apply to all flows
    flow = ~ifelse(flow == 0, NA, flow),
    hs   = ~as.numeric(hs),
    fcl  = ~as.integer(fcl),
    partner = ~as.integer(partner),
    # There are years == 0 and is.na. Making all of them is.na
    year = ~ifelse(year == "0", NA, year),
    year = ~as.integer(year) #,
    # value = ~ifelse(value == "VALUE_ORIG", "", value),
    # value = ~as.numeric(value),
    # special = ~as.numeric(special),
    # quantity.other = ~as.numeric(quantity.other)
  ) %>%
  left_join(tradeproc::mdbfaoareamap,
            by = c("reporter" = "mdbarea"))


test_that("All mdb area names are converted to fao area codes", {
  expect_equal(sum(is.na(adjustments$faoarea)), 0)})


adjustments <- adjustments %>%
  select_(~-reporter) %>%
  rename_(reporter = ~faoarea) %>%
  mutate_each_(funs(as.integer), list(~year, ~flow, ~fcl))

### Actions testing

# Quantity

# * A number: multiply it by quantity from data

test_that("Actions with numeric quantity have all other parts equal NA", {

  numeric_quant <- adjustments %>%
    filter_(~str_detect(quantity, "^[M,F,O]?\\d*\\.?\\d*$"))

  expect_equal(adjustments %>%
                filter_(~str_detect(quantity, "^[M,F,O]?\\d*\\.?\\d*$")) %>%
                filter_(~!(is.na(quantity.other) &
                          is.na(value) &
                          is.na(special))) %>%
                nrow,
               8)
#   reporter year flow         hs fcl partner quantity quantity.other value special faoarea
#   1      ISR   NA    4         NA 249      NA        2           <NA>  <NA>    0.25     105
#   2      SIN   NA   NA         NA 839      NA        0           <NA>  <NA>       0     200
#   3      SIN   NA   NA         NA 837      NA        0           <NA>  <NA>       0     200
#   4      SIN   NA   NA         NA 836      NA        0           <NA>  <NA>       0     200
#   5      USA   NA    2 1003004010  NA      NA    0.001           <NA>  <NA>   0.001     231
#   6      USA   NA    2 1007000020  NA      NA    0.001           <NA>  <NA>   0.001     231
#   7      USA   NA    2 1201000020  NA      NA    0.001           <NA>  <NA>   0.001     231
#   8      USA   NA    1 4101400010  NA      NA     1000           <NA>  <NA>   0.001     231
#
  # Claudia: They are statments, for that fcl/hs for that country,  valid every year.
})


test_that("Actions with quantity VALUE have quant.other and
          value equal NA and special is numeric", {
            quantity_value <- adjustments %>%
              filter_(~quantity == "VALUE")

            expect_equal(
              quantity_value %>%
                filter_(~!str_detect(special, "^\\d*\\.?\\d*$")) %>%
                nrow,
              0)

            expect_equal(
              quantity_value %>%
                filter_(~!is.na(special) &
                          is.na(quantity.other) &
                          is.na(value)) %>%
                nrow,
              nrow(quantity_value))
          })


test_that("Actions with quantity QUANTITY_ORIG have quant.other and
          value equal NA and special is numeric", {
            quantity_quantity_orig <- adjustments %>%
              filter_(~quantity == "QUANTITY_ORIG")

            expect_equal(
              quantity_quantity_orig %>%
                filter_(~!str_detect(special, "^\\d*\\.?\\d*$")) %>%
                nrow,
              0)

            expect_equal(
              quantity_quantity_orig %>%
                filter_(~!is.na(special) &
                          is.na(quantity.other) &
                          is.na(value)) %>%
                nrow,
              nrow(quantity_quantity_orig))
          })

test_that("All special are numeric", {
  expect_equal(
    adjustments %>%
      filter_(~!is.na(special)) %>%
      filter_(~!str_detect(special, "^\\d*\\.?\\d*$")) %>%
      nrow,
    0)
})

### Removing cases were weight and special are both numeric - so it is conflict

adjustments <- adjustments %>%
  mutate_(special = ~ifelse(reporter == 105 & fcl == 249 & flow == 4, NA, special)) %>%
  mutate_(special = ~ifelse(quantity == special &
                              str_detect(special, "^\\d*\\.?\\d*$"),
                            NA,
                            special)) %>%
  mutate_(special = ~ifelse(hs == 4101400010 & flow == 1 & reporter == 231,
                            NA,
                            special))



## Removing cases where no action is required ####
# * QUANTITY_ORIG: take quantity as is
# * ORACLE: leave as is

adjustments <- adjustments %>%
  filter_(~!(stringr::str_detect(quantity, "^QUANTITY_ORIG$|ORACLE_DATA") &
            is.na(quantity.other) & is.na(value) & is.na(special)))

adjustments <- adjustments %>%
  filter_(~!(stringr::str_detect(value, "^QUANTITY_ORIG$|ORACLE_DATA") &
            is.na(quantity.other) & is.na(quantity) & is.na(special)))


## Names as in trade dataset ####

adjustments <- adjustments %>%
  rename_(weight = ~quantity,
          qty = ~quantity.other) %>%
  mutate_each_(funs(tolower), vars = list(~weight,
                                          ~qty,
                                          ~value,
                                          ~special)) %>% # We have two ^M\\d* in special
  # Cases in qty
  # "QUANTITY_ORIG|QUANTITY_OTHER|VALUE_ORIG|VALUE|ORACLE_DATA|^[M,F,O]\\d*\\.?\\d*$"
  # All data is already orig
  mutate_(weight = ~stringr::str_replace(weight, "_orig$", "")) %>%
  mutate_(weight = ~ifelse(weight == "quantity_other", "qty", weight)) %>%
  mutate_(weight = ~ifelse(weight == "quantity", "weight", weight))


