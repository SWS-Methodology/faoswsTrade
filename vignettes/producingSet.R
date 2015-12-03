# Year for processing
year <- 2011

# Coefficient for outlier detection
# See coef argument in ?boxplot.stats
out_coef <- 1.5

debughsfclmap <- TRUE
multicore <- TRUE

library(tradeproc)
library(stringr)
suppressPackageStartupMessages(library(doParallel))
library(foreach)
library(testthat)
library(dplyr, warn.conflicts = F)

doParallel::registerDoParallel(cores=detectCores(all.tests=TRUE))


# Connection to SWS
# TODO: DEV MODE!!!!!!!!

faosws::GetTestEnvironment(
  # baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws", # intranet.fao.org/sws
  # baseUrl = "https://hqlprsws2.hq.un.fao.org:8181/sws",
  baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws", # QA?
  # token = "349ce2c9-e6bf-485d-8eac-00f6d7183fd6") # Token for QA)
  token = "da889579-5684-4593-aa36-2d86af5d7138") # http://hqlqasws1.hq.un.fao.org:8080/sws/
# token = "f5e52626-a015-4bbc-86d2-6a3b9f70950a") # Second token for QA
#token = token)


## Data sets with hs->fcl map (from mdb files)
# and UNSD area codes (M49)
## TODO: replace by ad hoc tables

data("hsfclmap2", package = "hsfclmap", envir = environment())
data("unsdpartnersblocks", package = "tradeproc", envir = environment())
data("unsdpartners", package = "tradeproc", envir = environment())



######### HS -> FCL map ############
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
  ## and add trailing 9 to tocode, where it is shorter
  ## TODO: check how many such cases and, if possible, move to manualCorrectoins
  mutate_(tocode = ~hsfclmap::trailingDigits(fromcode,
                                           tocode,
                                           digit = 9))



#### Max length of HS-codes in MDB-files ####

mapmaxlength <- hsfclmap %>%
  group_by_(~area, ~flow) %>%
  summarise_(mapmaxlength = ~max(stringr::str_length(fromcode))) %>%
  ungroup()


#### Get list of agri codes ####
agricodeslist <- paste0(shQuote(getAgriHSCodes(), "sh"), collapse=", ")

### Download TL data ####

tldata <- getRawAgriTL(year, agricodeslist)

#### Download ES data ####

esdata <- getRawAgriES(year, agricodeslist)

###### convert geonom to fao area list ####

esdata <- esdata %>%
  mutate_(reporter = ~convertGeonom2FAO(reporter),
          partner = ~convertGeonom2FAO(partner))

esdata <- convertHS2FCL(esdata, hsfclmap, parallel = multicore)

#### TL Converting area codes to FAO area codes ####
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

# Filtering out tradeflows reported by EU countires.
# They will be replaced by ES data

tldata <- tldata %>%
  anti_join(esdata %>%
              select_(~reporter) %>%
              distinct(),
            by = "reporter")

# We drop reporters what are absent in MDB hsfcl map
# because in any case we can proceed their data
tldata <- tldata %>%
  filter_(~reporter %in% unique(hsfclmap$area))


############# Lengths of HS-codes stuff ######################

### Reexport and reimport

# { "id": "1", "text": "Import" },
# { "id": "2", "text": "Export" },
# { "id": "4", "text": "re-Import" },
# { "id": "3", "text": "re-Export" }

tldata <- tldata %>%
  mutate_(flow = ~ifelse(flow == 4, 1L, ifelse(flow == 3, 2L, flow)))

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


########### Mapping HS codes to FCL in TL ###############

tldata <- convertHS2FCL(tldata %>%
                          select_(~-hs) %>%
                          rename_(hs = ~hsext),
                        hsfclmap1, parallel = multicore)



#############Units of measurment in TL ####

## Add target fclunit
# What units does FAO expects for given FCL codes

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
  filter_(~fclunit == "mt" & weight == 0 & conv == 0) %>%
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

tldata <- tldata %>%
  select_(~year,
          ~reporter,
          ~partner,
          ~flow,
          ~fcl,
          qty = ~qtyfcl, # <----
          ~value) %>%
  group_by_(~year, ~reporter, ~partner, ~flow, ~fcl) %>%
  summarise_each_(funs(sum(., na.rm = T)), vars = c("qty", "value")) %>%
  ungroup()


# Loading of notes/adjustments should be added here

esdata <- plyr::ldply(sort(unique(esdata$reporter)),
                          function(x) {
                            applyadj(x, year, adjustments, esdata)
                            },
                          .progress = ifelse(multicore, "none", "text"),
                          .inform = FALSE,
                          .parallel = multicore)

tradedata <- bind_rows(
  tldata %>%
    select_(~year, ~reporter, ~partner, ~flow,
            ~fcl, ~qty, ~value),
  # TODO Check quantity/weight
  esdata %>%
    select_(~year, ~reporter, ~partner, ~flow,
            ~fcl, ~qty, ~value)
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
    outlier = ~uv %in% boxplot.stats(uv, coef = out_coef, do.conf = F)) %>%
  ungroup()

# Imputation of missings and outliers

tradedata <- tradedata %>%
  mutate_(qty = ~ifelse(no_quant | outlier,
                      value / uv_reporter,
                      qty))


# Non reporting countries

nonreporting <- unique(tradedata$partner)[!is.element(unique(tradedata$partner),
                                                   unique(tradedata$reporter))]

tradedatanonrep <- tradedata %>%
  filter_(~partner %in% nonreporting) %>%
  mutate_(partner_mirr = ~reporter,
         reporter = ~partner,
         flow = ~ifelse(flow == 2, 1,
                       ifelse(flow == 1, 2,
                              NA)))

tradedata <- bind_rows(tradedata,
                       tradedatanonrep)
