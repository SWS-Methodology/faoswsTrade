load("tldata2011.RData")
load("usanotes2011.RData")

library(dplyr, warn.conflicts = F)
library(testthat)

# Transform notes to more friendly form
## Names correspond with tldata
## Remove redundant columns

adjustments <- usanotes2011 %>%
  select_(~flow,
          hs = ~original.item,
          fcl = ~item,
          partner = ~country,
          ~quantity,
          ~quantity.other,
          value = ~val,
          ~special,
          ~year)


test_that("All nonnumeric variants of quantity are expected", {
  expect_that(all(is.element(
    unique(adjustments$quantity[grepl("[a-zA-Z]",
                                      adjustments$quantity)]),
    # Vector of expected values in QUANTITY
    c("QUANTITY_ORIG",
      "M0",
      "QUANTITY_OTHER",
      "VALUE_ORIG",
      "VALUE"))),
    equals(TRUE))
})


test_that("All nonnumeric variants of value are expected", {
  expect_that(all(is.element(
    unique(adjustments$value[grepl("[a-zA-Z]",
                                      adjustments$value)]),
    # Vector of expected values in VALUE
    c("VALUE_ORIG"))),
    equals(TRUE))
})


test_that("All nonnumeric variants of quantity.other are expected", {
  expect_that(
    length(unique(adjustments$quantity.other[grepl("[a-zA-Z]",
                                   adjustments$quantity.other)])),
    equals(0)) # No nonnumeric values
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
    year = ~ifelse(year == 0, NA, year),
    year = ~as.integer(year),
    value = ~ifelse(value == "VALUE_ORIG", "", value),
    value = ~as.numeric(value),
    special = ~as.numeric(special),
    quantity.other = ~as.numeric(quantity.other))


# df <- data.frame(y = c(11, 12), f = c(1, 2), q = c(3, 6))
# within(df, q[y == 11 & f == 1] <- 33)
#
#
# adjustments %>%
#   filter(grepl("[a-zA-Z]", quantity.other))
#
# adjustments %>%
#   select(quantity.other) %>%
#   distinct()

adjustments %>%
  filter(grepl("[a-zA-Z]", quantity)) %>%
  group_by(quantity) %>%
  mutate(n_quant = n()) %>%
  ungroup() %>%
  mutate(q_w = 1/(n_quant / n())) %>%
  sample_n(10, weight = q_w) %>%
  arrange(quantity) %>%
  as.data.frame
