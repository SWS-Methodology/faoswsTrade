# Year for processing
year <- 2011

# Elements of path to sourced files with functions.
# They should be moved to a package
subdir <- "OrangeBook"
sourcedir <- "tradeR"


library(stringr)
library(hsfclmap)
library(tradeproc)
library(fclcpcmap)
suppressPackageStartupMessages(library(doParallel))
library(foreach)
registerDoParallel(cores=detectCores(all.tests=TRUE))
library(dplyr, warn.conflicts = F)


# Connection to SWS
# TODO: DEV MODE!!!!!!!!

source(file.path(Sys.getenv("HOME"),
                 "r_adhoc",
                 "trade_prevalid_testing",
                 "setupconnection.R"))

# Functions to get different M49 schemes from Web,
# MADB, missingIndicator, printTab
# TODO: should be in a package

if(length(
  lapply(
    dir(
      file.path(
        Sys.getenv("HOME"),
        "r_adhoc",
        "privateFAO",
        subdir,
        sourcedir),
      full.names = T),
    source)) == 0) stop("Files for sourcing not found")

## Data sets with hs->fcl map (from mdb files)
# and UNSD area codes (M49)
## TODO: replace by ad hoc tables

data("hsfclmap2", package = "hsfclmap")
data("unsdpartnersblocks", package = "tradeproc")
data("unsdpartners", package = "tradeproc")

agricodeslist <- paste0("'",
                        paste(getAgriHSCodes(),
                              collapse = "', '"),
                        "'")

tlsql <- paste0("
select * from (
select rep as reporter,
prt as partner,
comm as hs,
substring(comm from 1 for 6) as hs6,
flow,
tyear as year,
tvalue as value,
weight,
qty,
qunit
from ess.ct_tariffline_adhoc_unlogged) tbl1
where hs6 in (",
                agricodeslist
                ,
                ") and year = '",
                year,
                "'")

######### HS -> FCL map ############
## Filter hs->fcl links we need (based on year)

hsfclmap <- hsfclmap2 %>%
  filter_(~mdbyear == year &
         validyear %in% c(0, year)) %>%
  # Removing leading/trailing zeros from HS, else we get
  # NA during as.numeric()
  mutate_each_(funs(str_trim),
               c("fromcode", "tocode")) %>%
  # Convert flow to numbers for further joining with tlmaxlength
  mutate_(flow = ~ifelse(flow == "Import", 1L,
                         ifelse(flow == "Export", 2L,
                                NA))) %>%
## and add trailing 9 to tocode, where it is shorter
## TODO: check how many such cases and, if possible, move to manualCorrectoins
  mutate_(tocode = ~hsfclmap::trailingDigits(fromcode,
                                           tocode,
                                           digit = 9)) %>%
## Manual corrections of typos
    manualCorrections()


# Max length of HS-codes in MDB-files ####

mapmaxlength <- hsfclmap %>%
  group_by_(~area, ~flow) %>%
  summarise_(mapmaxlength = ~max(stringr::str_length(fromcode))) %>%

  ungroup()

### Extract TL data ####
## TODO: replace by call to SWS ad hoc

load("tldata.RData")

tldata <- tldata %>%
  mutate(hs = stringr::str_extract(hs, "^[0-9]*")) # Artifacts in reporters 646 and 208


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




### Reimport and reexport
# http://comtrade.un.org/data/cache/tradeRegimes.json
# { "id": "1", "text": "Import" },
# { "id": "2", "text": "Export" },
# { "id": "4", "text": "re-Import" },
# { "id": "3", "text": "re-Export" }

# tldata <- tldata %>%
#   group_by(year,
#            reporter,
#            partner,
#            hs,
#            qunit,
#            flow = ifelse(flow %in% c("1", "3"),
#                          "Import",
#                          "Export")) %>%
#   # Aggregation of RE-flows
#   summarize_each(funs(sum(., na.rm = T)),
#                  weight,
#                  qty,
#                  value) %>%  # We convert NA to zero here!!!!
#   ungroup()

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
                 scales::percent(sum(is.na(tldata$fcl))/nrow(tldata)),
                 "\nShare of value of tradeflows with nonmapped HS-codes in total value: ",
                 scales::percent(sum(tldata$value[is.na(tldata$fcl)], na.rm = T) /
                                   sum(tldata$value, na.rm = T))))

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

data("comtradeunits",
     package = "tradeproc",
     envir = environment())

tldata <- tldata %>%
  mutate_(qunit = ~as.integer(qunit)) %>%
  left_join(comtradeunits %>%
              select_(~qunit, ~wco),
            by = "qunit")

ctfclunitsconv <- tldata %>%
  select(qunit, wco, fclunit) %>%
  distinct() %>%
  arrange(qunit)

################ Conv. factor################


##### Table for conv. factor

ctfclunitsconv$conv <- 0
ctfclunitsconv$conv[ctfclunitsconv$qunit == 1] <- NA # Missing quantity
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "$ value only"] <- NA # Missing quantity
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "mt" &
                      ctfclunitsconv$wco == "m²"] <- 1
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "mt" &
                      ctfclunitsconv$wco == "l"] <- .001
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "heads" &
                      ctfclunitsconv$wco == "u"] <- 1
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "1000 heads" &
                      ctfclunitsconv$wco == "u"] <- .001
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "number" &
                      ctfclunitsconv$wco == "u"] <- 1
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "mt" &
                      ctfclunitsconv$wco == "kg"] <- .001
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "mt" &
                      ctfclunitsconv$wco == "m³"] <- 1
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "mt" &
                      ctfclunitsconv$wco == "carat"] <- 5e-6


##### Add conv factor to the dataset

tldata <- tldata %>%
  left_join(ctfclunitsconv,
            by = c("qunit", "wco", "fclunit"))

#### Commodity specific conversion

fcl_spec_mt_conv <- tldata %>%
  filter(fclunit == "mt" & weight == 0 & conv == 0) %>%
  select(fcl, wco) %>%
  distinct() %>%
  mutate(fcldesc = descFCL(fcl))

fcl_spec_mt_conv$convspec <- 0
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Cigarettes" &
                            fcl_spec_mt_conv$wco == "1000u"] <- .01
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Cigarettes" &
                            fcl_spec_mt_conv$wco == "u"] <- .0001
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Hen Eggs" &
                            fcl_spec_mt_conv$wco == "u"] <- .00006
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Hen Eggs" &
                            fcl_spec_mt_conv$wco == "2u"] <- .00012
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Hen Eggs" &
                            fcl_spec_mt_conv$wco == "12u"] <- .00072
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Hen Eggs" &
                            fcl_spec_mt_conv$wco == "1000u"] <- .006
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Cigars Cheroots" &
                            fcl_spec_mt_conv$wco == "u"] <- 0.000008
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Cigars Cheroots" &
                            fcl_spec_mt_conv$wco == "1000u"] <- 0.0008
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Tobacco Products nes" &
                            fcl_spec_mt_conv$wco == "u"] <- 0.000008
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Tobacco Products nes" &
                            fcl_spec_mt_conv$wco == "1000u"] <- 0.0008
fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Fruit Prepared nes" &
                            fcl_spec_mt_conv$wco == "U (jeu/pack)"] <- 0.0208333

### Add commodity specific conv.factors to dataset

tldata <- tldata %>%
  left_join(fcl_spec_mt_conv %>%
              select(-fcldesc),
            by = c("fcl", "wco"))


########## Conversion of units

#### FCL specific conv

tldata$qtyfcl <- tldata$qty * tldata$convspec

#### Common conv
# If no specific conv. factor, we apply general

tldata$qtyfcl <- ifelse(is.na(tldata$convspec),
                        tldata$qty * tldata$conv,
                        tldata$qtyfcl)

##### No qty, but weight and target is mt: we take weight from there

tldata$qtyfcl <- ifelse(tldata$qty == 0 &
                          tldata$fclunit == "mt" &
                          is.na(tldata$qtyfcl) &
                          tldata$weight > 0,
                        tldata$weight,
                        tldata$qtyfcl)


######### Value to thousands

tldata$value <- tldata$value / 1000

## aggregate by fcl

tldata <- tldata %>%
  select(year, reporter, partner, flow, fcl, qty = qtyfcl, value) %>%
  group_by(year, reporter, partner, flow, fcl) %>%
  summarise_each(funs(sum(., na.rm = T)), qty, value) %>%
  ungroup()
