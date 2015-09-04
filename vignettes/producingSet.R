library(dplyr, warn.conflicts = F)
library(stringr)
library(hsfclmap)
library(fclcpcmap)
suppressPackageStartupMessages(library(doParallel))
library(foreach)
registerDoParallel(cores=detectCores(all.tests=TRUE))

subdir <- "OrangeBook"
sourcedir <- "tradeR"


source(file.path(Sys.getenv("HOME"), "r_adhoc", "trade_prevalid_testing", "setupconnection.R"))

if(length(lapply(dir(file.path(Sys.getenv("HOME"), "r_adhoc", "privateFAO", subdir, sourcedir),
                     full.names = T),
                 source)) == 0) stop("Files for sourcing not found")

data("hsfclmap")

data("unsdpartnersblocks", package = "tradeproc")
data("unsdpartners", package = "tradeproc")

# source(file.path(Sys.getenv("HOME"), ".pwd.R"))

trade_src <- src_postgres("sws_data", "localhost", 5432, "trade", .pwd,
                          options = "-c search_path=ess")

agri_db <- tbl(trade_src, sql("
                              select * from ess.agri
                              "))


hsfclmap <- hsfclmap %>%
  mutate(tocode = trailingDigits(fromcode, tocode, digit = 9),
         validyear = as.integer(validyear),
         validyear = ifelse(is.na(validyear), 0, validyear)) %>%
  manualCorrections() %>%
  mutate(fao = as.integer(fao))


# Max length of HS-codes in MDB-files

mapmaxlength <- hsfclmap %>%
  group_by(fao, flow) %>%
  summarise(mapmaxlength = max(str_length(fromcode))) %>%
  ungroup()





### Extract TL data and calculate length of hs codes


tldata <- agri_db %>%
  select(-hs2, -hs4, -hs6) %>%
  filter(year == "2011",
         flow %in% c("1", "2")) %>% #Drop reexport and reimport
  collect() %>%
  mutate(reporter = as.integer(reporter),
         partner = as.integer(partner)) %>%
  mutate(hs = stringr::str_extract(hs, "^[0-9]*")) %>% # Artifacts in reporters 646 and 208
  ## Converting area codes to FAO
  left_join(unsdpartnersblocks %>%
              select(wholepartner = rtCode,
                     part = formula) %>%
              filter(wholepartner %in% c(251, 381, 579, 581, 711, 757, 842)), # Exclude EU and old countries
            by = c("partner" = "part")) %>%
  mutate(partner = ifelse(is.na(wholepartner), partner, wholepartner)) %>%
  ## Aggregation of numbers for FAO areas
  ## We could aggregate later after convertion to FCL commodities
  group_by(year, reporter, partner, flow, hs, qunit) %>%
  summarize_each(funs(sum(., na.rm = T)), weight, qty, value) %>% # We convert NA to zero here!!!!
  ungroup() %>%
  mutate(flow = as.character(factor(flow, labels = c("Import", "Export")))) %>%
  group_by(reporter, flow) %>%
  mutate(tlmaxlength = max(stringr::str_length(hs), na.rm = T)) %>%
  ungroup() %>%
  mutate(reporter = tradeproc::convertTLParnterToFAO(reporter),
         partner = tradeproc::convertTLParnterToFAO(partner))


## Max length in TL


tlmaxlength <- tldata %>%
  select(reporter,
         flow,
         tlmaxlength) %>%
  group_by(reporter, flow) %>%
  summarize(tlmaxlength = max(tlmaxlength, na.rm = T)) %>%
  ungroup()


## Common max length


maxlengthdf <- tlmaxlength %>%
  left_join(mapmaxlength,
            by = c("reporter" = "fao", "flow")) %>%
  group_by(reporter, flow) %>%
  mutate(maxlength = max(tlmaxlength, mapmaxlength, na.rm = T)) %>%
  # na.rm here: some reporters are absent in map
  #  122 145 180 224 276
  ungroup()



### Extension of HS-codes in TL


tldata <- tldata %>%
  select(-tlmaxlength) %>%
  left_join(maxlengthdf %>%
              select(-tlmaxlength, -mapmaxlength),
            by = c("reporter", "flow")) %>%
  mutate(hsext = as.numeric(trailingDigits2(hs,
                                            maxlength = maxlength,
                                            digit = 0)))

### Extension of HS ranges in map


hsfclmap1 <- hsfclmap %>%
  left_join(maxlengthdf %>%
              select(-tlmaxlength, -mapmaxlength),
            by = c("fao" = "reporter", "flow")) %>%
  filter(!is.na(maxlength))                                         ## Attention!!!

hsfclmap1 <- hsfclmap1 %>%
  mutate(fromcode = as.numeric(trailingDigits2(fromcode, maxlength, 0)),
         tocode = as.numeric(trailingDigits2(tocode, maxlength, 9)))


## Mapping HS codes to FCL

df <- tldata %>%
  select(reporter, flow, hsext) %>%
  distinct()

fcldf <- hsInRange(df$hsext, df$reporter, df$flow, hsfclmap1,
                   calculation = "grouping",
                   parallel = T)

print(paste0("Proportion of nonmapped HS-codes: ", sum(is.na(fcldf$fcl))/nrow(fcldf)))

## Adding FCL to main TL data set
#
tldata <- tldata %>%
  left_join(fcldf,
            by = c("reporter" = "areacode", "flow" = "flowname", "hsext" = "hs"))

print(paste0("Proportion of tradeflows with nonmapped HS-codes: ", sum(is.na(tldata$fcl))/nrow(tldata)))


## Unite weight with qty and aggregate by fcl

tldata <- tldata %>%
  mutate(kgyes = !is.na(weight) & weight > 0,
         qunit = ifelse(kgyes, 8, qunit),
         qty = ifelse(kgyes, weight, qty)) %>%
  select(year, reporter, partner, flow, fcl, qunit, qty, value) %>%
  group_by(year, reporter, partner, flow, fcl, qunit) %>%
  summarise_each(funs(sum(., na.rm = T)), qty, value) %>%
  ungroup()
