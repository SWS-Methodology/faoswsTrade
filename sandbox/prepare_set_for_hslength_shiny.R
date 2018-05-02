dataexportdir <- file.path(Sys.getenv("HOME"),
                           "r_adhoc",
                           "hslength_shiny")

library(dplyr, warn.conflicts = F)
library(stringr)
library(hsfclmap)
library(fclcpcmap)

subdir <- "OrangeBook"
sourcedir <- "tradeR"


source(file.path(Sys.getenv("HOME"), "r_adhoc", "trade_prevalid_testing", "setupconnection.R"))

if(length(lapply(dir(file.path(Sys.getenv("HOME"), "r_adhoc", "privateFAO", subdir, sourcedir),
                     full.names = T),
                 source)) == 0) stop("Files for sourcing not found")

data("hsfclmap2", package = "hsfclmap")

trade_src <- src_postgres("sws_data", "localhost", 5432, "trade", .pwd,
                          options = "-c search_path=ess")

agri_db <- tbl(trade_src, sql("
                              select * from ess.agri
                              "))

data("FAOcountryProfile", package = "FAOSTAT")
faonames <- FAOcountryProfile %>%
  select(faoarea = FAOST_CODE, name = SHORT_NAME)


## MDB

hsfclmap <- hsfclmap2 %>%
  left_join(faonames,
            by = c("area" = "faoarea")) %>%
  select(-area) %>%
  rename(reporter = name)

maplength <- hsfclmap %>%
  mutate(hslength = stringr::str_length(fromcode)) %>%
  group_by(year = mdbyear, reporter, flow, hslength) %>%
  summarise(n_map = n()) %>%
  ungroup()


map_alllengths <- maplength %>%
  group_by(year, reporter, flow) %>%
  summarize(lengthsinmap = paste(sort(unique(hslength)), collapse = ",")) %>%
  ungroup()


## tariff-line

agri_df <- agri_db %>%
  select(year, reporter, flow, hs, value) %>%
  collect() %>%
  mutate(reporter = tradeproc::convertComtradeM49ToFAO(reporter),
         reporter = as.integer(reporter),
         year = as.integer(year),
         hslength = stringr::str_length(hs),
         flow = ifelse(flow %in% c(1, 3),
                       "Import",
                       "Export")) %>%
  left_join(faonames,
            by = c("reporter" = "faoarea")) %>%
  select(-reporter) %>%
  rename(reporter = name)



hslength <- agri_df %>%
  # Info about trailing zeros
  mutate(trailzero2 = str_detect(hs, "0{2}$"),
         trailzero4 = str_detect(hs, "0{4}$")) %>%
  group_by(year, reporter, flow) %>%
  mutate(trailzero2 = sum(trailzero2) / n(),
         trailzero4 = sum(trailzero4) / n()) %>%
  group_by(year, reporter, flow, trailzero2, trailzero4, hslength) %>%
  summarize(n_tl = n(),
            value = sum(value, na.rm = T)) %>%
  group_by(year, reporter, flow) %>%
  mutate(flowsprop = n_tl / sum(n_tl),
         valueprop = value / sum(value)) %>%
  ungroup()

tl_alllengths <- hslength %>%
  group_by(year, reporter, flow) %>%
  summarize(lengthsintl = paste(sort(unique(hslength)), collapse = ",")) %>%
  ungroup()

# Join

tl_map_length <- hslength %>%
  full_join(maplength,
            by = c("year", "reporter", "flow", "hslength")) %>%
  select(year, reporter, flow, hslength, n_tl, n_map,
         flowsprop, valueprop, trailzero2, trailzero4) %>%
  filter(is.na(n_map) | is.na(n_tl)) %>%
  mutate_each(funs(ifelse(is.na(.), 0, .)),
              n_tl,
              n_map,
              flowsprop,
              valueprop) %>%
  mutate_each(funs(round(100 * ., 1)),
              flowsprop,
              valueprop,
              trailzero2,
              trailzero4) # %>%
#   mutate_each(funs(as.factor),
#               flow,
#               reporter)

tl_map_length <- tl_map_length %>%
  left_join(tl_alllengths,
            by = c("year", "reporter", "flow")) %>%
  left_join(map_alllengths,
            by = c("year", "reporter", "flow")) %>%
  mutate_each(funs(ifelse(is.na(.), "NA", .)),
                   starts_with("lengthsin"))


agri_df <- agri_df %>%
  inner_join(tl_map_length %>%
               select(year, reporter, flow),
             by = c("year", "reporter", "flow"))

tl_map_length <- tl_map_length %>%
  mutate_each(funs(as.factor),
              reporter,
              flow)

save(tl_map_length, file = file.path(dataexportdir, "tl_map_length.Rdata"))
save(agri_df, file = file.path(dataexportdir, "agri_df.Rdata"))
save(hsfclmap, file = file.path(dataexportdir, "hsfclmap.Rdata"))

DBI::dbDisconnect(trade_src$con)
