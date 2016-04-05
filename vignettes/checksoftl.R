faoreps <- c(203, 79, 84)


library(dplyr, warn.conflicts = F)
library(stringr)

subdir <- "OrangeBook"
sourcedir <- "tradeR"


source(file.path(Sys.getenv("HOME"), "r_adhoc", "trade_prevalid_testing", "setupconnection.R"))

if(length(lapply(dir(file.path(Sys.getenv("HOME"), "r_adhoc", "privateFAO", subdir, sourcedir),
                     full.names = T),
                 source)) == 0) stop("Files for sourcing not found")




trade_src <- src_postgres("sws_data", "localhost", 5432, "trade", .pwd,
                          options = "-c search_path=ess")

agri_db <- tbl(trade_src, sql("
                              select * from ess.agri
                              "))

# Germany example

# Let's extract it from TL

agri_db %>%
  select(-hs2, -hs4, -hs6) %>%
  filter(reporter == "276") %>%
  collect() %>%
  mutate(hslength = str_length(hs)) %>%
  group_by(year, flow, hslength) %>%
  summarize(n_flows = n())


# Greece

agri_db %>%
  select(-hs2, -hs4, -hs6) %>%
  filter(reporter == "300") %>%
  collect() %>%
  mutate(hslength = str_length(hs)) %>%
  group_by(year, flow, hslength) %>%
  summarize(n_flows = n())

# Spain 724

agri_db %>%
  select(-hs2, -hs4, -hs6) %>%
  filter(reporter == "724") %>%
  collect() %>%
  mutate(hslength = str_length(hs)) %>%
  group_by(year, flow, hslength) %>%
  summarize(n_flows = n())

.ojdbcclasspath <- file.path(Sys.getenv("HOME"), "dstrbs", "ojdbc14.jar")

tfsource <- fclhs::gettfsource(reporter = faoreps, year = 2011)
tfsource %>%
  mutate(hslength = str_length(hs)) %>%
  group_by(reporter, flow, hslength) %>%
  summarize(n_flows = n())

## Specific cases


agri_db %>%
  select(-hs2, -hs4) %>%
  filter(reporter == "276" & hs6 == "120510" & partner == "250" & year == "2011" & flow == "1") %>%
  group_by(year, reporter, partner, hs, flow) %>%
  summarize_each(funs(sum(.) / 1000), weight, qty, value)

tfsource %>%
  filter(reporter == 79 & partner == 68 & flow == 1 &
           str_extract(hs, "^.{6}") == "120510") %>%
  group_by(reporter, partner, hs6 = str_extract(hs, "^.{6}"), flow) %>%
  summarize_each(funs(sum), qty, qty2, value)


tfsource %>%
  filter(reporter == 84) %>%
  top_n(10, qty)


agri_db %>%
  select(-hs2, -hs4) %>%
  filter(reporter == "300" & hs6 == "120510" & partner == "250" & year == "2011" & flow == "1") %>%
  group_by(year, reporter, partner, hs, flow) %>%
  summarize_each(funs(sum(.) / 1000), weight, qty, value)

tfsource %>%
  filter(reporter == 84 & partner == 68 & flow == 1 &
           str_extract(hs, "^.{6}") == "100190") %>%
  group_by(reporter, partner, hs6 = str_extract(hs, "^.{6}"), flow) %>%
  summarize_each(funs(sum), qty, qty2, value)

## Total value

tfsource11 <- fclhs:::faostatsql(
  "select * from (
           select REPORT_AREA as reporter,
           PARTNER_AREA as partner,
           FLOW,
           trim(ITEM_ORIG) as hs,
           QUANTITY_ORIG as qty,
           OTHER_QUANTITY as qty2,
           VALUE_ORIG as value
   from FAOSTAT.TF_SOURCE_2011)")

tfsource11 %>%
  filter(flow == 1) %>%
  group_by(reporter) %>%
  summarize(value = sum(value)/1000) %>%
  top_n(10) %>%
  arrange(desc(value))

agri_db %>%
  filter(year == "2011" & flow == "1") %>%
  group_by(reporter) %>%
  summarize(value = sum(value) / 1000) %>%
  top_n(10) %>%
  arrange(desc(value))


## QTY

fclhs:::faostatsql(
  "select * from (
   select reporter, sum(qty) / 1000 as qty from (
           select REPORT_AREA as reporter,
           FLOW,
           QUANTITY_ORIG as qty
   from FAOSTAT.TF_SOURCE_2011
   where flow = 1)
   group by reporter )
   order by qty desc
   ")



tfsource11 %>%
  filter(flow == 1) %>%
  group_by(reporter) %>%
  summarize(qty = sum(qty)/1000) %>%
  top_n(10) %>%
  arrange(desc(qty))

agri_db %>%
  filter(year == "2011" & flow == "1") %>%
  group_by(reporter) %>%
  summarize(weight = sum(weight) / 1000) %>%
  ungroup() %>%
  collect() %>%
  filter(!is.na(weight)) %>%
  top_n(10) %>%
  arrange(desc(weight))


## Wheat in US  - not a problem

agri_db %>%
  filter(flow == "1" & hs6 == "100190" & reporter == "842" & partner == "392")


tfsource11 %>%
  filter(flow == 1 & reporter == 231 & partner == 110 &
           str_extract(hs, "^.{6}") == "100190")

fclhs::gettfsource(reporter = 231, year = 2010, partner = 110, flow = 1) %>%
  filter(str_extract(hs, "^.{6}") == "100190")

fclhs::gettfsource(reporter = 231, year = 2009, partner = 110, flow = 1) %>%
  filter(str_extract(hs, "^.{6}") == "100190")
