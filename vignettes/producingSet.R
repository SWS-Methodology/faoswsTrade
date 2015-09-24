year <- 2011


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

data("hsfclmap2", package = "hsfclmap")

data("unsdpartnersblocks", package = "tradeproc")
data("unsdpartners", package = "tradeproc")

trade_src <- src_postgres("sws_data", "localhost", 5432, "trade", .pwd,
                          options = "-c search_path=ess")

agri_db <- tbl(trade_src, sql("
                              select * from ess.agri
                              "))


hsfclmap <- hsfclmap2 %>%
  filter(mdbyear == year,
         validyear %in% c(0, year)) %>%
  mutate(tocode = hsfclmap::trailingDigits(fromcode,
                                           tocode,
                                           digit = 9)) %>%
  manualCorrections()


# Max length of HS-codes in MDB-files

mapmaxlength <- hsfclmap %>%
  group_by(area, flow) %>%
  summarise(mapmaxlength = max(str_length(fromcode))) %>%
  ungroup()

### Extract TL data

tldata <- agri_db %>%
  select(-hs2, -hs4, -hs6) %>%
  filter(year == "2011") %>%
  collect() %>%
  mutate(reporter = as.integer(reporter),
         partner = as.integer(partner)) %>%
  mutate(hs = stringr::str_extract(hs, "^[0-9]*")) # Artifacts in reporters 646 and 208

### Reimport and reexport
# http://comtrade.un.org/data/cache/tradeRegimes.json
# { "id": "1", "text": "Import" },
# { "id": "2", "text": "Export" },
# { "id": "4", "text": "re-Import" },
# { "id": "3", "text": "re-Export" }

tldata <- tldata %>%
  group_by(year,
           reporter,
           partner,
           hs,
           qunit,
           flow = ifelse(flow %in% c("1", "3"),
                         "Import",
                         "Export")) %>%
  summarize_each(funs(sum(., na.rm = T)),
                 weight,
                 qty,
                 value) %>%  # We convert NA to zero here!!!!
  ungroup()


### Converting area codes to FAO

tldata <- tldata %>%
  left_join(unsdpartnersblocks %>%
              select(wholepartner = rtCode,
                     part = formula) %>%
              filter(wholepartner %in% c(251, 381, 579, 581, 711, 757, 842)), # Exclude EU and old countries
            by = c("partner" = "part")) %>%
  mutate(partner = ifelse(is.na(wholepartner), partner, wholepartner)) %>%
  ## Aggregation of numbers for joined M49 areas
  ## We could aggregate later after convertion to FCL commodities
  group_by(year, reporter, partner, flow, hs, qunit) %>%
  summarize_each(funs(sum(., na.rm = T)), weight, qty, value) %>% # We convert NA to zero here!!!!
  ungroup() %>%
  mutate(m49rep = reporter,
         m49par = partner,
         reporter = tradeproc::convertComtradeM49ToFAO(m49rep),
         partner = tradeproc::convertTLParnterToFAO(partner))

# Nonmapped M49 partner codes: 251, 381, 473, 490,
# 527, 568, 577, 579, 581, 637, 711, 757, 837, 838, 839, 842, 899


if(any(is.na(tldata$reporter)))
  message(paste0(
    "Nonmapped M49 reporter codes: ",
    paste(sort(unique(tldata$m49rep[is.na(tldata$reporter)])), collapse = ", "),
    "\nProportion of trade flows with nonmapped M49 reporter codes: ",
    scales::percent(sum(is.na(tldata$reporter))/nrow(tldata))))

if(any(is.na(tldata$partner)))
  message(paste0(
    "Nonmapped M49 partner codes: ",
    paste(sort(unique(tldata$m49par[is.na(tldata$partner)])), collapse = ", "),
    "\nProportion of trade flows with nonmapped M49 partner codes: ",
    scales::percent(sum(is.na(tldata$partner))/nrow(tldata))))


############# Lengths of HS-codes stuff ######################
###  Calculate length of hs codes

tldata <- tldata %>%
  group_by(reporter, flow) %>%
  mutate(tlmaxlength = max(stringr::str_length(hs), na.rm = T)) %>%
  ungroup()


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
            by = c("reporter" = "area", "flow")) %>%
  group_by(reporter, flow) %>%
  mutate(maxlength = max(tlmaxlength, mapmaxlength, na.rm = T)) %>%
  # na.rm here: some reporters are absent in map
  #  122 145 180 224 276
  ungroup()


####
# hsnotequal.R




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
            by = c("area" = "reporter", "flow")) %>%
  filter(!is.na(maxlength))                                         ## Attention!!!

hsfclmap1 <- hsfclmap1 %>%
  mutate(fromcode = as.numeric(trailingDigits2(fromcode, maxlength, 0)),
         tocode = as.numeric(trailingDigits2(tocode, maxlength, 9)))


########### Mapping HS codes to FCL ###############

df <- tldata %>%
  select(reporter, flow, hsext) %>%
  distinct()

fcldf <- hsInRange(df$hsext, df$reporter, df$flow, hsfclmap1,
                   calculation = "grouping",
                   parallel = T)

if(any(is.na(fcldf$fcl)))
  message(paste0("Proportion of nonmapped HS-codes: ",
                 scales::percent(sum(is.na(fcldf$fcl))/nrow(fcldf))))

## Adding FCL to main TL data set
#
tldata <- tldata %>%
  left_join(fcldf,
            by = c("reporter" = "areacode", "flow" = "flowname", "hsext" = "hs"))

if(any(is.na(tldata$fcl)))
  message(paste0("Proportion of tradeflows with nonmapped HS-codes: ",
                 scales::percent(sum(is.na(tldata$fcl))/nrow(tldata))))

#############Units of measurment ##################

## Add target fclunit

## units for fcl
data("fclunits",
     package = "tradeproc",
     envir = environment())

tldata <- tldata %>%
  left_join(fclunits,
            by = "fcl")


## Units of Comtrade



## Add conv. factor

##


## Unite weight with qty and aggregate by fcl

tldata <- tldata %>%
  mutate(kgyes = !is.na(weight) & weight > 0,
         qunit = ifelse(kgyes, 8, qunit),
         qty = ifelse(kgyes, weight, qty)) %>%
  select(year, reporter, partner, flow, fcl, qunit, qty, value) %>%
  group_by(year, reporter, partner, flow, fcl, qunit) %>%
  summarise_each(funs(sum(., na.rm = T)), qty, value) %>%
  ungroup()
