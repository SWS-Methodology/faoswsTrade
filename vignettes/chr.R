# File with some tests

## SQL

library(DBI)
library(dplyr)
library(RPostgreSQL)

password <- 'XXX'

m <- dbDriver("PostgreSQL")
con <- dbConnect(m,
                 user     = "sws_ro", 
                 password = password, 
                 dbname   = "sws_data",
                 host     = "hqldvpostgres01.hq.un.fao.org",
                 port     = 5420)

pcon <- src_postgres(user     = "sws_ro", 
                     password = password, 
                     dbname   = "sws_data",
                     host     = "hqldvpostgres01.hq.un.fao.org",
                     port     = 5420,
                     options  = "-c search_path=ess")

pcon %>% tbl("ct_tariffline_unlogged_2011")

hs_chapters <- c(1:24, 33, 35, 38, 40:41, 43, 50:53) %>%
  formatC(width = 2, format = "d", flag = "0") %>%
  as.character


pcon %>%
  tbl("ct_tariffline_unlogged_2011") %>%
  filter(comm %in% hs_chapters) %>%
  mutate(
         cases  = if_else(is.na(qty), 10, 0) + if_else(is.na(weight), 1, 0),
         nrows = 1,
         hs6 = sql('substring(comm, 1, 6)')
         ) %>%
        group_by(year, rep, prt, flow, hs, hs6, qunit, cases) %>%
        summarise_each(
                        funs(sum(.)),
                        vars = c(tvalue, weight, qty, nrows)) %>%
        ungroup()




