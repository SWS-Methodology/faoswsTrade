

# ---- settings ----

# Year for processing
year <- 2009L

# Coefficient for outlier detection
# See coef argument in ?boxplot.stats
out_coef <- 1.5

debughsfclmap <- TRUE
multicore <- TRUE

# ---- libs ----

#library(tradeproc)
library(faoswsTrade)
library(stringr)
library(testthat)
library(dplyr, warn.conflicts = F)

if(multicore) {
  suppressPackageStartupMessages(library(doParallel))
  library(foreach)
  doParallel::registerDoParallel(cores=detectCores(all.tests=TRUE))
}

# ---- swsdebug ----

# Connection to SWS
# TODO: DEV MODE!!!!!!!!

## ADDED COMMENT
# faosws::GetTestEnvironment(
#   # baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws", # intranet.fao.org/sws
#   # baseUrl = "https://hqlprsws2.hq.un.fao.org:8181/sws",
#   baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws", # QA?
#   # token = "349ce2c9-e6bf-485d-8eac-00f6d7183fd6") # Token for QA)
#   token = "da889579-5684-4593-aa36-2d86af5d7138") # http://hqlqasws1.hq.un.fao.org:8080/sws/
# # token = "f5e52626-a015-4bbc-86d2-6a3b9f70950a") # Second token for QA
# #token = token)

# ---- datasets ----
## Data sets with hs->fcl map (from mdb files)
# and UNSD area codes (M49)
## TODO: replace by ad hoc tables

data("hsfclmap2", package = "hsfclmap", envir = environment())
data("adjustments", package = "hsfclmap", envir = environment())
#data("unsdpartnersblocks", package = "tradeproc", envir = environment())
data("unsdpartnersblocks", package = "faoswsTrade", envir = environment())
#data("unsdpartners", package = "tradeproc", envir = environment())
data("unsdpartners", package = "faoswsTrade", envir = environment())
## units for fcl
#data("fclunits", package = "tradeproc", envir = environment())
data("fclunits", package = "faoswsTrade", envir = environment())

# ---- hsfclmapsubset ----
# HS -> FCL map
## Filter hs->fcl links we need (based on year)

hsfclmap <- hsfclmap2 %>%
  # Filter out all records from future years
  filter_(~mdbyear <= year) %>%
  # Distance from year of interest to year in the map
  mutate_(yeardistance = ~year - mdbyear) %>%
  # Select nearest year for every reporter
  # if year == 2011 and mdbyear == 2011, then distance is 0
  # if year == 2011 and mdbyear == 2010, distance is 1
  group_by_(~area) %>%
  filter_(~yeardistance == min(yeardistance)) %>%
  ungroup() %>%
  select_(~-yeardistance) %>%
  ## and add trailing 9 to tocode, where it is shorter
  ## TODO: check how many such cases and, if possible, move to manualCorrectoins
  mutate_(tocode = ~hsfclmap::trailingDigits(fromcode,
                                             tocode,
                                             digit = 9))



# ---- data from Giorgio (from drive) ----

# ---- tradeload ----

#### Get list of agri codes ####
### ADDED COMMENT
## agricodeslist <- paste0(shQuote(getAgriHSCodes(), "sh"), collapse=", ")

### Download TL data ####

# tldata <- getRawAgriTL(year, agricodeslist)

## This require a function to access the SWS, at the moment
## the data are download from google drive
load(paste0("~/Desktop/FAO/Trade/RData/tldata_",year,".RData"))
tldata = tldata_raw

#### Download ES data ####

# esdata <- getRawAgriES(year, agricodeslist)
#load("../esdata_raw_from_db.RData")
#load("~/Dropbox/tradeproc/esdata_raw_from_db.RData")
load(paste0("~/Desktop/FAO/Trade/RData/esdata_",year,".RData"))
esdata = esdata_raw

## To be discussed with Michael:
## Preanalysis has to be added here or not?

# ---- geonom2fao ----


esdata <- esdata %>%
  mutate_(reporter = ~convertGeonom2FAO(reporter),
          partner = ~convertGeonom2FAO(partner)) %>%
  filter_(~partner != 252)

# ---- es_hs2fcl ----
esdata <- convertHS2FCL(esdata, hsfclmap, parallel = multicore)

# ---- es_join_fclunits ----

esdata <- esdata %>%
  left_join(fclunits, by = "fcl")


# ---- tl_m49fao ----
## Based on Excel file from UNSD (unsdpartners..)

tldata <- tldata %>%
  left_join(unsdpartnersblocks %>%
              select_(wholepartner = ~rtCode,
                      part = ~formula) %>%
              # Exclude EU grouping and old countries
              filter_(~wholepartner %in% c(251, 381, 579, 581, 711, 757, 842)),
            by = c("partner" = "part")) %>%
  mutate_(partner = ~ifelse(is.na(wholepartner), partner, wholepartner),
          m49rep = ~reporter,
          m49par = ~partner,
          # Conversion from Comtrade M49 to FAO area list
          reporter = ~as.integer(tradeproc::convertComtradeM49ToFAO(m49rep)),
          partner = ~as.integer(tradeproc::convertComtradeM49ToFAO(m49par)))

# ---- drop_es_from_tl ----
# They will be replaced by ES data

tldata <- tldata %>%
  anti_join(esdata %>%
              select_(~reporter) %>%
              distinct(),
            by = "reporter")



# ---- drop_reps_not_in_mdb ----
# We drop reporters what are absent in MDB hsfcl map
# because in any case we can proceed their data
tldata <- tldata %>%
  filter_(~reporter %in% unique(hsfclmap$area))



# ---- tl_hslength ----

#### Max length of HS-codes in MDB-files ####

mapmaxlength <- hsfclmap %>%
  group_by_(~area, ~flow) %>%
  summarise_(mapmaxlength = ~max(stringr::str_length(fromcode))) %>%
  ungroup()

###  Calculate length of hs codes in TL

tldata <- tldata %>%
  group_by_(~reporter, ~flow) %>%
  mutate_(tlmaxlength = ~max(stringr::str_length(hs), na.rm = TRUE)) %>%
  ungroup()

## Dataset with max length in TL ####

tlmaxlength <- tldata %>%
  select_(~reporter,
          ~flow,
          ~tlmaxlength) %>%
  group_by_(~reporter, ~flow) %>%
  summarize_(tlmaxlength = ~max(tlmaxlength, na.rm = TRUE)) %>%
  ungroup()

## Common max length between TL and map ####

maxlengthdf <- tlmaxlength %>%
  left_join(mapmaxlength,
            by = c("reporter" = "area", "flow")) %>%
  group_by_(~reporter, ~flow) %>%
  mutate_(maxlength = ~max(tlmaxlength, mapmaxlength, na.rm = TRUE)) %>%
  # na.rm here: some reporters are absent in map
  #  122 145 180 224 276
  ungroup()

### Extension of HS-codes in TL ####

tldata <- tldata %>%
  select_(~-tlmaxlength) %>%
  left_join(maxlengthdf %>%
              select_(~-tlmaxlength, ~-mapmaxlength),
            by = c("reporter", "flow")) %>%
  mutate_(hsext = ~as.numeric(hsfclmap::trailingDigits2(hs,
                                                        maxlength = maxlength,
                                                        digit = 0)))

### Extension of HS ranges in map ####


hsfclmap1 <- hsfclmap %>%
  left_join(maxlengthdf %>%
              select_(~-tlmaxlength, ~-mapmaxlength),
            by = c("area" = "reporter", "flow")) %>%
  filter_(~!is.na(maxlength))                                         ## Attention!!!

hsfclmap1 <- hsfclmap1 %>%
  mutate_(fromcode = ~as.numeric(hsfclmap::trailingDigits2(fromcode, maxlength, 0)),
          tocode = ~as.numeric(hsfclmap::trailingDigits2(tocode, maxlength, 9)))


# ---- tl_hs2fcl ----

tldata <- convertHS2FCL(tldata %>%
                          select_(~-hs) %>%
                          rename_(hs = ~hsext),
                        hsfclmap1, parallel = multicore)

#############Units of measurment in TL ####

## Add target fclunit
# What units does FAO expects for given FCL codes

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

## Dataset with all matches between Comtrade and FAO units
ctfclunitsconv <- tldata %>%
  select_(~qunit, ~wco, ~fclunit) %>%
  distinct() %>%
  arrange_(~qunit)

################ Conv. factor (TL) ################


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
## This is just a trick
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "1000 heads" &
                      ctfclunitsconv$wco == "kg"] <- 1
## This one too :)
ctfclunitsconv$conv[ctfclunitsconv$fclunit == "heads" &
                      ctfclunitsconv$wco == "kg"] <- 1

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
  filter_(~fclunit == "mt" & is.na(weight) & conv == 0) %>%
  select_(~fcl, ~wco) %>%
  distinct() %>%
  mutate_(fcldesc = ~descFCL(fcl))

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
              select_(~-fcldesc),
            by = c("fcl", "wco"))

## Save intermediate dataset for checks
tldata_old = tldata

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


######### Value from USD to thousands of USD
tldata$value <- tldata$value / 1000

## TLDATA: aggregate by fcl
## Here we select column qtyfcl which contains quantity, requested by FAO

# tldata <- tldata %>%
#   select_(~year,
#           ~reporter,
#           ~partner,
#           ~flow,
#           ~fcl,
#           qty = ~qtyfcl, # <----
#           ~value) %>%
#   group_by_(~year, ~reporter, ~partner, ~flow, ~fcl) %>%
#   summarise_each_(funs(sum(., na.rm = T)), vars = c("qty", "value")) %>%
#   ungroup()

# Replace weight (first quantity column) by newly produced qtyfcl column
tldata <- tldata %>%
  select_(~year,
          ~reporter,
          ~partner,
          ~flow,
          ~fcl,
          ~hs,
          weight = ~qtyfcl,
          ~qty,
          ~value)

tldata_mid = tldata

# Loading of notes/adjustments should be added here
esdata_old = esdata

esdata <- plyr::ldply(sort(unique(esdata$reporter)),
                      function(x) {
                        applyadj(x, year, adjustments, esdata)
                      },
                      .progress = ifelse(multicore, "none", "text"),
                      .inform = FALSE,
                      .parallel = multicore)

## Apply conversion EUR to USD
load("data/EURconversionUSD.RData")
esdata$value <- esdata$value * as.numeric(EURconversionUSD %>%
                                            filter(Year == year) %>%
                                            select(ExchangeRate))

tldata <- plyr::ldply(sort(unique(tldata$reporter)),
                      function(x) {
                        applyadj(x, year, adjustments, tldata)
                      },
                      .progress = ifelse(multicore, "none", "text"),
                      .inform = FALSE,
                      .parallel = multicore)

tradedata <- bind_rows(
  tldata %>%
    select_(~year, ~reporter, ~partner, ~flow,
            ~fcl, qty = ~weight, ~value),
  # TODO Check quantity/weight
  esdata %>%
    mutate_(uniqqty = ~ifelse(fclunit == "mt", weight, qty)) %>%
    select_(~year, ~reporter, ~partner, ~flow,
            ~fcl, qty = ~uniqqty, ~value)
)


tradedata <- tradedata %>%
  select_(~year, ~reporter, ~partner, ~flow, ~fcl, ~qty, ~value) %>%
  group_by_(~year, ~reporter, ~partner, ~flow, ~fcl) %>%
  summarise_each_(funs(sum = sum(., na.rm = TRUE)), vars = c("qty", "value")) %>%
  ungroup()

# Missing quantities and values

tradedata <- tradedata %>%
  mutate_(no_quant = ~qty == 0 | is.na(qty),  # There are no NA qty, but may be changes later
          no_value = ~value == 0 | is.na(value))


# UV calculation

tradedata <- mutate_(tradedata,
                     uv = ~ifelse(no_quant | no_value, # Only 0 here. Should we care about NA?
                                  NA,
                                  value / qty))

# Outlier detection

tradedata <- tradedata %>%
  group_by_(~year, ~reporter, ~flow, ~fcl) %>%
  mutate_(
    uv_reporter = ~median(uv, na.rm = T),
    outlier = ~uv %in% boxplot.stats(uv, coef = out_coef, do.conf = F)$out) %>%
  ungroup()

# Imputation of missings and outliers
tradedata <- tradedata %>%
  mutate_(qty = ~ifelse(no_quant | outlier,
                        value / uv_reporter,
                        qty))

# Non reporting countries
nonreporting <- unique(tradedata$partner)[!is.element(unique(tradedata$partner),
                                                      unique(tradedata$reporter))]

## Mirroring for non reporting countries
tradedatanonrep <- tradedata %>%
  filter_(~partner %in% nonreporting) %>%
  mutate_(partner_mirr = ~reporter,
          reporter = ~partner,
          flow = ~ifelse(flow == 2, 1,
                         ifelse(flow == 1, 2,
                                NA)))

tradedata <- bind_rows(tradedata,
                       tradedatanonrep)


## Non mapped FCL
tldata_old %>%
  filter(is.na(fcl)) %>%
  select(hs6) %>%
  unique() %>%
  write.table(file = "non_mapped_fcl.csv",
              row.names = F,quote = F)


## Rule 4 "order of magnitude"
#https://github.com/mkao006/sws_r_api/blob/040fb7f7b6af05ec35293dd5459ee131b31e5856/r_modules/trade_prevalidation/R/magnitudeOrder.R

## Kernel calculation
#https://github.com/mkao006/sws_r_api/blob/040fb7f7b6af05ec35293dd5459ee131b31e5856/r_modules/trade_prevalidation/R/kdeMode.R

## Simple reliability index
#https://github.com/mkao006/sws_r_api/blob/040fb7f7b6af05ec35293dd5459ee131b31e5856/r_modules/trade_reliability/simpleReliabilityExampleForDocumentation.R

