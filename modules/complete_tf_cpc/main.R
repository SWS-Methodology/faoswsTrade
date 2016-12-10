##' ## Complete TF CPC
##'
##' **Author: Alex Matrunich, Marco Garieri, Bo Werth, Christian Mongeau**
##'
##' **Description:**
##'
##' The trade module is divided in two submodules: **complete\_tf\_cpc** and
##' **total\_trade\_CPC**. Each module is year specific. This means that, at the
##' time being, the trade module run indipendently for each year. In order to
##' run the **tt total\_trade\_CPC**, the output of **complete\_tf\_cpc** is
##' needed.
##'
##' Change Log:
##'
##' - add unit values to output
##' - remove adjustment factors
##' - revise flags: add **flagObservationStatus** `X` and **flagMethod** `c`, `i`

##+ init, echo=FALSE, eval=FALSE

## **Flow chart:**
##
## ![Aggregate complete_tf to total_trade](assets/diagram/trade_3.png?raw=true "livestock Flow")

set.seed(2507)

debughsfclmap <- TRUE
multicore <- FALSE

## If true, the reported values will be in $
## If false the reported values will be in k$
dollars <- FALSE

## If TRUE, use adjustments (AKA "conversion notes")
use_adjustments <- FALSE


##+ libs, echo=FALSE, eval=FALSE

## library(tradeproc)

suppressPackageStartupMessages({
  library(data.table)
  library(faoswsTrade)
  library(faosws)
  library(stringr)
  library(scales)
  library(faoswsUtil)
  library(tidyr)
  library(dplyr, warn.conflicts = F)
})


if(!CheckDebug()){

  options(error = function(){
    dump.frames()
    SWS_USER = regmatches(swsContext.username,
                          regexpr("(?<=/).+$", swsContext.username, perl = TRUE))
    filename <- file.path(Sys.getenv("R_SWS_SHARE_PATH"), SWS_USER, "complete_tf_cpc")
    dir.create(filename, showWarnings = FALSE, recursive = TRUE)
    save(last.dump, file=file.path(filename, "last.dump.RData"))
  })
}

PID <- Sys.getpid()

## Check that all packages are up to date
local({
  min_versions <- data.frame(package = c("faoswsUtil", "faoswsTrade",
                                         "dplyr"),
                             version = c('0.2.11', '0.1.1', '0.5.0'),
                             stringsAsFactors = FALSE)

  for (i in nrow(min_versions)){
    # installed version
    p <- packageVersion(min_versions[i,"package"])
    # required version
    v <- package_version(min_versions[i,"version"])
    if(p < v){

      stop(sprintf("%s >= %s required", min_versions[i,"package"], v))
    }
  }

})


if(multicore) {
  suppressPackageStartupMessages(library(doParallel))
  library(foreach)
  doParallel::registerDoParallel(cores=detectCores(all.tests=TRUE))
}


##+ swsdebug, echo=FALSE, eval=FALSE

## ## local data
## install.packages("//hqfile4/ess/Team_working_folder/A/SWS/faosws_0.8.2.9901.tar.gz",
##                  repos = NULL,
##                  type = "source")
## ## SWS data
## install.packages("faosws",
##                  repos = "http://hqlprsws1.hq.un.fao.org/fao-sws-cran/")

if(CheckDebug()){
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/complete_tf_cpc/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}


##+ settings, echo=FALSE, eval=FALSE

stopifnot(
  !is.null(swsContext.computationParams$year),
  !is.null(swsContext.computationParams$out_coef))

# Year for processing
year <- as.integer(swsContext.computationParams$year)

## List of datasets available
#datas = faosws::FetchDatatableConfig()


# Coefficient for outlier detection
# See coef argument in ?boxplot.stats
out_coef <- as.numeric(swsContext.computationParams$out_coef)

startTime = Sys.time()

##' ### Input Data
##'
##' **Supplementary Datasets:**
##'
##' 1. `hsfclmap2`: Mmapping between HS and FCL codes extracted from MDB files
##' used to archive information existing in the previous trade system (Shark,
##' Jellyfish).
##'
##' 2. `adjustments`: Adjustment notes containing manually added conversion
##' factors to obtain quantities from traded values
##'
##' 3. `unsdpartnersblocks`: UNSD Tariffline reporter and partner dimensions use
##' different list of geographic are codes. The partner dimesion is more
##' detailed than the reporter dimension. Since we can not split trade flows of
##' the reporter dimension, trade flows of the corresponding partner dimensions
##' have to be assigned the reporter dimension's geographic area code. For
##' example, the code 842 is used for the United States includes Virgin Islands
##' and Puerto Rico and thus the reported trade flows of those territories.
##' Analogous steps are taken for France, Italy, Norway, Switzerland and US
##' Minor Outlying Islands.
##'
##' 4. `fclunits`: For UNSD Tariffline units of measurement are converted to
##' meet FAO standards. According to FAO standard, all weights are reported in
##' metric tonnes, animals in heads or 1000 heads and for certain commodities,
##' only the value is provided.
##'
##' 5. `comtradeunits`:
##'
##' 6. `EURconversionUSD`: Annual EUR/USD currency exchange rates table from SWS

##+ datasets, echo=FALSE, eval=FALSE

## Old procedure
#data("hsfclmap2", package = "hsfclmap", envir = environment())
## New procedure
message(sprintf("[%s] Reading in hs-fcl mapping", PID))
hsfclmap2 <- tbl_df(ReadDatatable("hsfclmap2"))
## Old precedure
#data("adjustments", package = "hsfclmap", envir = environment())
## New procedure
message(sprintf("[%s] Reading in adjustments", PID))

adjustments <- tbl_df(ReadDatatable("adjustments"))
colnames(adjustments) = sapply(colnames(adjustments),
                               function(x) gsub("adj_","",x))
adj_cols_int <- c("year","flow","fcl","partner","reporter")
adj_cols_dbl <- c("hs")
adjustments = adjustments %>%
  mutate_each_(funs(as.integer),adj_cols_int) %>%
  mutate_each_(funs(as.double),adj_cols_dbl)

adjustments = adjustments %>%
  filter_(~!(is.na(year) &
               weight == 1000))
#  filter_(~!(flow == 2 &
#             reporter == 231 &
#             hs %in% c(1001100090,1001902015,1001902055)))

warning("Notes without specific year and with a multiplier factor
        of 1000 have been deleted because were redundant.
        This needs additional study anyway.
        This problem is referenced in Github as issue #35.")

###adjustments = adjustments %>%
###  mutate_(hs = ~as.double(adjustments))
## Old procedure
data("unsdpartnersblocks", package = "faoswsTrade", envir = environment())
#unsdpartnersblocks <- tbl_df(ReadDatatable("unsdpartnersblocks"))
## units for fcl old procedure
data("fclunits", package = "faoswsTrade", envir = environment())
#fclunits <- tbl_df(ReadDatatable("fclunits"))
## units of Comtrade old procedure
data("comtradeunits", package = "faoswsTrade", envir = environment())
#comtradeunits <- tbl_df(ReadDatatable("comtradeunits"))
## Eur to USD
data("EURconversionUSD", package = "faoswsTrade", envir = environment())
#EURconversionUSD <- tbl_df(ReadDatatable("eur_conversion_usd"))

##+ hsfclmapsubset, echo=FALSE, eval=FALSE
# HS -> FCL map
## Filter hs->fcl links we need (based on year)

hsfclmap <- hsfclmapSubset(hsfclmap2, year = year)

##' #### Extract UNSD Tariffline Data
##'
##' 1. Chapters: The module downloads only records of commodities of interest for Tariffline
##' Data. The HS chapters are the following: 01, 02, 03, 04, 05, 06, 07, 08, 09,
##' 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 33, 35, 38, 40,
##' 41, 42, 43, 50, 51, 52, 53. In the future, if other commotidy are of
##' interest for the division, it is important to include additional chapter in
##' the first step of the downloading. For Eurostat Data no filtering is
##' applied.

##+ tradeload, echo=FALSE, eval=FALSE

#### Get list of agri codes ####
#agricodeslist <- paste0(shQuote(getAgriHSCodes(), "sh"), collapse=", ")

### Download TL data ####

# tldata <- getRawAgriTL(year, agricodeslist)

message(sprintf("[%s] Reading in Tariffline data", PID))

## Chapter provided by team B/C
## creating object to provision re-use with Eurostat data
hs_chapters <- c(1:24, 33, 35, 38, 40:43, 50:53)
hs_chapters_str <-
  formatC(hs_chapters, width = 2, format = "d", flag = "0") %>%
  as.character %>%
  shQuote(type = "sh") %>%
  paste(collapse = ", ")

tldata <- ReadDatatable(paste0("ct_tariffline_unlogged_",year),
                        columns=c("rep", "tyear", "flow",
                                  "comm", "prt", "weight",
                                  "qty", "qunit", "tvalue",
                                  "chapter"),
                        where = paste0("chapter IN (", hs_chapters_str, ")")
                        )


##' 2. Remove non-numeric comm (hs) code; comm (hs) code has to be digit.
##' This probably should be part of the faoswsEnsure

##+ tl-force-numeric-comm, echo=FALSE, eval=FALSE

tldata <- tldata[grepl("^[[:digit:]]+$",tldata$comm),]

tldata <- tbl_df(tldata)

##' 3. The tariffline data from UNSD contains multiple rows with identical
##' combination of reporter / partner / commodity / flow / year / qunit. Those
##' are separate registered transactions and the rows containinig non-missing
##' values and quantities are summed.
##'
##' 4. **Note:** missing quantity|weight or value will be handled below by imputation

##+ tl-aggregate-multiple-rows, echo=FALSE, eval=FALSE

## Aggregate multiple TL rows.
## Note: missing quantity|weight or value will be handled below by imputation
tldata <- preAggregateMultipleTLRows(tldata)

## Rename columns
tldata <- adaptTradeDataNames(tradedata = tldata, origin = "TL")

##' #### Extract Eurostat Combined Nomenclature Data
##'
##' 1. Remove reporters with area codes that are not included in MDB commodity
##' mapping area list
##' 2. Convert HS to FCL
##' 3. Remove unmapped FCL codes
##' 4. Join *fclunits*
##' 5. `NA` *fclunits* set to `mt`
##' 6. Specific ES conversions: some FCL codes are reported in Eurostat
##' with different supplementary units than those reported in FAOSTAT

##+ es-extract, echo=FALSE, eval=FALSE
#### Download ES data ####

# esdata <- getRawAgriES(year, agricodeslist)
#load("../esdata_raw_from_db.RData")
#load("~/Dropbox/tradeproc/esdata_raw_from_db.RData")
#load(paste0("~/Desktop/FAO/Trade/RData/esdata_",year,".RData"))
#esdata = esdata_raw

message(sprintf("[%s] Reading in Eurostat data", PID))
esdata <- ReadDatatable(paste0("ce_combinednomenclature_unlogged_",year),
                        columns = c("declarant", "partner",
                                    "product_nc", "flow",
                                    "period", "value_1k_euro",
                                    "qty_ton", "sup_quantity",
                                    "stat_regime"),
                        where = paste0("chapter IN (", hs_chapters_str, ")")
)

## Declarant and partner numeric
## This probably should be part of the faoswsEnsure
esdata <- esdata[grepl("^[[:digit:]]+$",esdata$declarant),]
esdata <- esdata[grepl("^[[:digit:]]+$",esdata$partner),]
## Removing TOTAL from product_nc column
esdata <- esdata[grepl("^[[:digit:]]+$",esdata$product_nc),]
## Only regime 4 is relevant for Eurostat data
esdata <- esdata[esdata$stat_regime=="4",]
## Removing stat_regime as it is not needed anymore
esdata[,stat_regime:=NULL]

esdata <- tbl_df(esdata)

## Rename columns
esdata <- adaptTradeDataNames(tradedata = esdata, origin = "ES")

##+ geonom2fao, echo=FALSE, eval=FALSE
esdata <- data.table::as.data.table(esdata)
esdata[, `:=`(reporter = convertGeonom2FAO(reporter),
              partner = convertGeonom2FAO(partner))]
esdata <- esdata[partner != 252, ]
esdata <- tbl_df(esdata)


##+ es-treat-unmapped, echo=FALSE, eval=FALSE
esdata_not_area_in_fcl_mapping <- esdata %>%
  filter_(~!(reporter %in% unique(hsfclmap$area)))
esdata <- esdata %>%
  filter_(~reporter %in% unique(hsfclmap$area))

## es_hs2fcl
message(sprintf("[%s] Convert Eurostat HS to FCL", PID))
esdata <- convertHS2FCL(esdata, hsfclmap, parallel = multicore)

## es remove non mapped fcls
esdata_fcl_not_mapped <- esdata %>%
  filter_(~is.na(fcl))

esdata <- esdata %>%
  filter_(~!(is.na(fcl)))

## es join fclunits
esdata <- addFCLunits(tradedata = esdata, fclunits = fclunits)

## specific supplementary unit conversion
es_spec_conv <- frame_data(
  ~fcl, ~conv,
  1057L, 0.001,
  1068L, 0.001,
  1072L, 0.001,
  1079L, 0.001,
  1083L, 0.001,
  1140L, 0.001,
  1181L, 1000
)

esdata <- esdata %>%
  left_join(es_spec_conv, by='fcl') %>%
  mutate_(qty=~ifelse(is.na(conv), qty, qty*conv)) %>%
  select_(~-conv)

##' #### Harmonize UNSD Tariffline Data
##'
##' 1. Geographic Area: UNSD Tariffline data reports area code with Tariffline M49 standard
##' (which are different for official M49). The area code is converted in FAO
##' country code using a specific convertion table provided by Team ENV. Area
##' codes not mapping to any FAO country code or mapping to code 252 (which
##' correpond not defined area) are separately saved and removed from further
##' analyses.
##'
##' 2. Commodity Codes: Commodity codes are reported in HS
##' codes (Harmonized Commodity Description and Coding Systpem). The codes
##' are converted in FCL (FAO Commodity List) codes. This step is performed
##' using table incorporated in the SWS. In this step, all the mapping between
##' HS and FCL code is stored. If a country is not included in the package of
##' the mapping for that specific year, all the records for the reporting
##' country are removed. All records without an FCL mapping are filtered out and
##' saved in specific variables.

##+ tl_m49fao, echo=FALSE, eval=FALSE
## Based on Excel file from UNSD (unsdpartners..)

message(sprintf("[%s] Converting from comtrade to FAO codes", PID))

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
          reporter = ~as.integer(faoswsTrade::convertComtradeM49ToFAO(m49rep)),
          partner = ~as.integer(faoswsTrade::convertComtradeM49ToFAO(m49par)))


##+ drop_es_from_tl, echo=FALSE, eval=FALSE
# They will be replaced by ES data

tldata <- tldata %>%
  anti_join(esdata %>%
              select_(~reporter) %>%
              distinct(),
            by = "reporter")

##+ drop_reps_not_in_mdb, echo=FALSE, eval=FALSE
# We drop reporters what are absent in MDB hsfcl map
# because in any case we can proceed their data

tldata_not_area_in_fcl_mapping <- tldata %>%
  filter_(~!(reporter %in% unique(hsfclmap$area)))

tldata <- tldata %>%
  filter_(~reporter %in% unique(hsfclmap$area))


##+ reexptoexp, echo=FALSE, eval=FALSE

# { "id": "1", "text": "Import" },
# { "id": "2", "text": "Export" },
# { "id": "4", "text": "re-Import" },
# { "id": "3", "text": "re-Export" }

tldata <- tldata %>%
  mutate_(flow = ~recode(flow, '4' = 1L, '3' = 2L))


##+ tl_hslength, echo=FALSE, eval=FALSE

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
  mutate_(hsext = ~as.numeric(trailingDigits2(hs,
                                              maxlength = maxlength,
                                              digit = 0)))

### Extension of HS ranges in map ####

hsfclmap1 <- hsfclmap %>%
  left_join(maxlengthdf %>%
              select_(~-tlmaxlength, ~-mapmaxlength),
            by = c("area" = "reporter", "flow")) %>%
  filter_(~!is.na(maxlength))                                         ## Attention!!!

hsfclmap1 <- hsfclmap1 %>%
  mutate_(fromcode = ~as.numeric(trailingDigits2(fromcode, maxlength, 0)),
          tocode = ~as.numeric(trailingDigits2(tocode, maxlength, 9)))


##+ tl_hs2fcl, echo=FALSE, eval=FALSE

tldata <- convertHS2FCL(tldata %>%
                          select_(~-hs) %>%
                          rename_(hs = ~hsext),
                        hsfclmap1, parallel = multicore)


## Non mapped FCL
tldata_fcl_not_mapped <- tldata %>%
  filter_(~is.na(fcl))

tldata <- tldata %>%
  filter_(~!(is.na(fcl)))

#############Units of measurment in TL ####

## Add target fclunit

tldata <- addFCLunits(tradedata = tldata, fclunits = fclunits)

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
  filter_(~fclunit == "mt" & is.na(weight) & conv == 0) %>%
  select_(~fcl, ~wco) %>%
  distinct() %>%
  mutate_(fcldesc = ~descFCL(fcl))

if(NROW(fcl_spec_mt_conv) > 0){

  conversion_factors_fcl <- tldata %>%
    filter(!is.na(weight) & !is.na(qty)) %>%
    mutate(qw=(weight/qty)/1000) %>%
    group_by(fcl, wco) %>%
    summarise(convspec=median(qw, na.rm=TRUE)) %>%
    ungroup()
  
  fcl_spec_mt_conv <- fcl_spec_mt_conv %>%
    left_join(conversion_factors_fcl)
    
  fcl_spec_mt_conv$convspec[is.na(fcl_spec_mt_conv$convspec)] <- 0

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
} else {
  tldata$qtyfcl = NA
}



##### No qty, but weight and target is mt: we take weight from there

tldata$qtyfcl <- ifelse((tldata$qty == 0 | is.na(tldata$qty)) &
                          tldata$fclunit == "mt" &
                          is.na(tldata$qtyfcl) &
                          tldata$weight > 0,
                        tldata$weight,
                        tldata$qtyfcl)

# Always use weight if available and fclunit is mt
tldata$qtyfcl <- ifelse(tldata$fclunit=='mt' & !is.na(tldata$weight) & tldata$weight>0,
                       tldata$weight*0.001,
                       tldata$qtyfcl)

######### Value from USD to thousands of USD
if (dollars){
  esdata$value <- esdata$value * 1000
} else { ## This means it is in k$
  tldata$value <- tldata$value / 1000
}

##' 3. Aggregate UNSD Tariffline Data to FCL: here we select column `qtyfcl`
##' which contains weight in tons (requested by FAO).

##+ tl_aggregate, echo=FALSE, eval=FALSE

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
          ~fclunit,
          ~hs,
          weight = ~qtyfcl,
          ~qty,
          ~value)

tldata_mid = tldata

##' #### Combine Trade Data Sources
##'
##' 1. The adjustment notes developed for national data received from countries
##' are not applied to HS data any more (see instructions 2016-08-10). Data
##' harvested from UNSD are standardised and therefore many (if not most) of the
##' quantity adjustment notes (those with no year) need not be applied. The
##' "notes" refer to the "raw" non-standardised files that we used to regularly
##' receive from UNSD and/or the countries. Furthermore, some data differences
##' will also arise due to more recent data revisions in these latest files that
##' have been harvested.

##+ apply_adjustment, echo=FALSE, eval=FALSE

# TODO Check quantity/weight
# The notes should save the results in weight

if (use_adjustments == TRUE) {
  esdata <- useAdjustments(tradedata = esdata, year = year, PID = PID,
                           adjustments = adjustments, parallel = multicore)

  tldata <- useAdjustments(tradedata = tldata, year = year,
                           adjustments = adjustments, parallel = multicore)
}

##' 2. Convert currency of monetary values from EUR to USD using the
##' `EURconversionUSD` table (see above).

##+ es_convcur, echo=FALSE, eval=FALSE
## Apply conversion EUR to USD
esdata$value <- esdata$value * as.numeric(EURconversionUSD %>%
                                            filter(Year == year) %>%
                                            select(ExchangeRate))

##' 3. Combine UNSD Tariffline and Eurostat Combined Nomenclature data sources
##'  to single data set.
##'  - TL: assign `weight` to `qty`
##'  - ES: assign `weight` to `qty` if `fclunit` is equal to `mt`, else keep `qty`

##+ combine_es_tl, echo=FALSE, eval=FALSE

tradedata <- bind_rows(
  tldata %>%
    # Not using as.character() as it will retain scientific notation
    mutate_(hs = ~format(hs, scientific = FALSE, trim = TRUE)) %>%
    select_(~year, ~reporter, ~partner, ~flow,
            ~fcl, ~fclunit, ~hs,
            qty = ~weight, ~value),
  esdata %>%
    mutate_(uniqqty = ~ifelse(fclunit == "mt", weight, qty)) %>%
    select_(~year, ~reporter, ~partner, ~flow,
            ~fcl, ~fclunit,~hs,
            qty = ~uniqqty, ~value)
)


##' #### Outlier Detection and Imputation

##' 1. Unit values are calculated for each observation at the HS level as ratio
##' of monetary value over weight `value / qty`.
##'
##' 2. Median unit-values are calculated across the partner dimension by year,
##' reporter, flow and HS. This can be problematic if only few records exist for
##' the a specific combination of dimensions.

##+ calculate_median_uv, echo=FALSE, eval=FALSE

tradedata <- tradedata %>%
  mutate_(no_quant = ~near(qty, 0) | is.na(qty),  # There are no any NA qty records,
                                              # but may change later
          no_value = ~near(value, 0) | is.na(value))


## UV calculation
tradedata <- mutate_(tradedata,
                     uv = ~ifelse(no_quant | no_value, NA, value / qty))

## Round UV in order to avoid floating point number problems (see issue #54)
tradedata$uv <- round(tradedata$uv, 10)

##' 3. Observations are classified as outliers if the calculated unit value for
##' a some partner country is below or above the median unit value. More
##' specifically, the measure defined as median inter-quartile-range (IQR)
##' multiplied by the outlier coefficient (default value: 1.5) is used to
##' categorize outlier observations.

##+ boxplot_uv, echo=FALSE, eval=FALSE

## Outlier detection

tradedata <- detectOutliers(tradedata = tradedata, method = "boxplot",
                            parameters = list(out_coef=out_coef))

##' 4. Impute missing quantities and quantities categorized as outliers by
##' dividing the reported monetary value with the calculated median unit value.
##'
##' 5. Assign `flagTrade` to observations with imputed quantities. These flags
##' are also assigned to monetary values. This may need to be revised (monetary
##' values are not supposed to be modified).
##'
##' 6. Aggregate by FCL over HS dimension: reduce from around 15000 commodity
##' codes to around 800 commodity codes.
##'
##' 7. Map FCL codes to CPC, remove observations that have not been mapped to
##' CPC.

##+ impute_qty_uv, echo=FALSE, eval=FALSE

# Imputation of missings and outliers

tradedata <- doImputation(tradedata = tradedata)

# Aggregation by fcl
tradedata <- tradedata %>%
  select_(~year,
          ~reporter,
          ~partner,
          ~flow,
          ~fcl,
          ~fclunit,
          ~qty,
          ~value,
          ~flagTrade) %>%
  group_by_(~year, ~reporter, ~partner, ~flow, ~fcl, ~fclunit) %>%
  summarise_each_(funs(sum(., na.rm = TRUE)),
                  vars = c("qty", "value","flagTrade")) %>%
  ungroup()

# Adding CPC2 extended code
tradedata <- tradedata %>%
  mutate_(cpc = ~fcl2cpc(sprintf("%04d", fcl), version = "2.1"))

# Not resolve mapping fcl2cpc
no_mapping_fcl2cpc = tradedata %>%
  select_(~fcl, ~cpc) %>%
  filter_(~is.na(cpc)) %>%
  distinct_(~fcl) %>%
  select_(~fcl) %>%
  unlist()

# Converting back to M49 for the system
tradedata <- tradedata %>%
  mutate_(reporterM49 = ~fs2m49(as.character(reporter)),
          partnerM49 = ~fs2m49(as.character(partner)))

# Report of countries mapping to NA in M49
# 2011: fal 252: "Unspecified" in FAOSTAT area list
countries_not_mapping_M49 <- bind_rows(
  tradedata %>% select_(fc = ~reporter, m49 = ~reporterM49),
  tradedata %>% select_(fc = ~partner, m49 = ~partnerM49)) %>%
  distinct_() %>%
  filter_(~is.na(m49)) %>%
  select_(~fc) %>%
  unlist()


##' #### Mirror Trade Estimation
##'
##' 1. Obtain list of non-reporting countries as difference between the list of
##' reporter countries and the list of partner countries.
##'
##' 2. Swap the reporter and partner dimensions: the value previously appearing
##' as reporter country code becomes the partner country code (and vice versa).
##'
##' 3. Invert the flow direction: an import becomes an export (and vice versa).
##'
##' 4. Calculate monetary mirror value by adding a 12% mark-up on imports to
##' account for the difference between CIF and FOB prices.

##+ mirror_estimation, echo=FALSE, eval=FALSE

# Non reporting countries
nonreporting <- unique(tradedata$partner)[!is.element(unique(tradedata$partner),
                                                      unique(tradedata$reporter))]

## Mirroring for non reporting countries
tradedata <- mirrorNonReporters(tradedata = tradedata,
                                nonreporters = nonreporting)

##' 5. Reporting countries: Assign SWS **observationStatus** flag `I` and
##' **flagMethod** `e` to records with with `flagTrade` unless the FCL unit is
##' categorized as `$ value only`.
##'
##' 6. Non-reporting countries: Assign SWS **observationStatus** flag `E` and
##' **flagMethod** `e` to both quantities and values. Overwrite **flagMethod**
##' `e` with `c` for quantities when transforming to normalized format below.

##+ sws_flag, echo=FALSE, eval=FALSE

## Flag from numeric to letters
## TO DO (Marco): need to discuss how to treat flags
##                at the moment Status I and method e
##                for both imputed and mirrored
##                because applying 12% change in mirroring

addFlagsAfterMirror <- function(data=stop("'data' must be defined'"),
                                nonreporting=NULL) {

  ## data <- tradedata
  copyData <- data

  outData <-
    copyData %>%
    mutate_(
      flagObservationStatus =
        ## ~ifelse((flagTrade > 0) & (fclunit != "$ value only"),
        ~ifelse(reporter %in% nonreporting,
                "E",
                ifelse((flagTrade > 0) & (fclunit != "$ value only"),
                       "I",
                       "X")
                ),
      flagMethod =
        ~ifelse(reporter %in% nonreporting,
                "e", # both measures in same row; need to overwrite with "c"
                     # flag for quantities when transforming to normalized
                     # format
                ifelse((flagTrade > 0) & (fclunit != "$ value only"),
                       "e",
                       "")
                )
    )

    return(outData)
}

## complete_trade <- tradedata %>%
##   mutate_(
##     flagObservationStatus = ~ifelse((flagTrade > 0) &
##                                       (fclunit != "$ value only"),"I",""),
##     flagMethod = ~ifelse((flagTrade > 0) &
##                            (fclunit != "$ value only"),"e",""))

complete_trade <-
  tradedata %>% addFlagsAfterMirror(nonreporting = nonreporting)

##' #### Output for SWS
##'
##' 1. Filter observations with FCL code `1181` (bees).
##'
##' 2. Filter observations with missing CPC codes.
##'
##' 3. Rename dimensions to comply with SWS standard, e.g. `geographicAreaM49Reporter`
##'
##' 4. Calculate unit value (US$ per quantity unit) at CPC level if the quantity is larger than zero

##+ completed_trade_flow, echo=FALSE, eval=FALSE

complete_trade_flow_cpc <- complete_trade %>%
  filter_(~fcl != 1181) %>% ## Subsetting out bees
  select_(~-fcl) %>%
  filter_(~!(is.na(cpc))) %>%
  transmute_(geographicAreaM49Reporter = ~reporterM49,
             geographicAreaM49Partner = ~partnerM49,
             flow = ~flow,
             timePointYears = ~year,
             flagObservationStatus = ~flagObservationStatus,
             flagMethod = ~flagMethod,
             measuredItemCPC = ~cpc,
             qty = ~qty,
             unit = ~fclunit,
             value = ~value) %>%
  ## unit of monetary values is "1000 $"
  mutate(uv = ifelse(qty > 0, value * 1000 / qty, NA))


##' 4. Transform dataset seperating monetary values, quantities and unit values
##' in different rows.
##'
##' 5. Convert monetary values, quantities and unit values to corresponding SWS
##' element codes. For example, a quantity import measured in metric tons is
##' assigned `5610`.

##+ convert_element, echo=FALSE, eval=FALSE

complete_trade_flow_cpc <- complete_trade_flow_cpc %>%
  tidyr::gather(measuredElementTrade, Value, -geographicAreaM49Reporter,
                -geographicAreaM49Partner, -measuredItemCPC,
                -timePointYears, -flagObservationStatus,
                -flagMethod, -unit, -flow) %>%
  rowwise() %>%
  mutate_(measuredElementTrade =
            ~convertMeasuredElementTrade(measuredElementTrade,
                                         unit,
                                         flow)) %>%
  ungroup() %>%
  filter_(~measuredElementTrade != "999") %>%
  select_(~-flow,~-unit)# %>%
#mutate_(flagTrade = ~flagObservationStatus) %>%
#select_(~-flagMethod, ~-flagObservationStatus)

##' 6. Overwrite **flagMethod** for mirrored quantities: `e` becomes `c`

##+ overwrite_mirror_method_flag, echo=FALSE, eval=FALSE

overwriteFlagMethodMirrorQuantities <- function(data=stop("'data' cannot be empty"),
                                                quantityElements=c("5608", "5609", "5610", "5908", "5909", "5910")) {
  copyData <- data
  outData <-
    copyData %>%
    mutate_(flagMethod =
              ~ifelse(flagObservationStatus == "E" & measuredElementTrade %in% quantityElements,
                      "c",
                      flagMethod)
            )
  return(outData)
}

complete_trade_flow_cpc <-
  complete_trade_flow_cpc %>%
  overwriteFlagMethodMirrorQuantities()


##' 7. Add **flagMethod** `i` to unit values

##+ add_uv_method_flag, echo=FALSE, eval=FALSE

addFlagUnitValues <- function(data=stop("'data' cannot be empty'"),
                              uvElements=c("5638", "5639", "5630", "5938", "5939", "5930")) {
  copyData <- data
  outData <-
    copyData %>%
    mutate_(flagMethod =
              ~ifelse(measuredElementTrade %in% uvElements,
                      "i",
                      flagMethod))
  return(outData)
}

complete_trade_flow_cpc <-
  complete_trade_flow_cpc %>%
  addFlagUnitValues()

## table(complete_trade_flow_cpc$flagObservationStatus,
##       complete_trade_flow_cpc$flagMethod)


complete_trade_flow_cpc <- data.table::as.data.table(complete_trade_flow_cpc)

data.table::setcolorder(complete_trade_flow_cpc,
                        c("geographicAreaM49Reporter",
                          "geographicAreaM49Partner",
                          "measuredElementTrade",
                          "measuredItemCPC",
                          "timePointYears",
                          "Value",
                          "flagObservationStatus",
                          "flagMethod"))

message(sprintf("[%s] Writing data to session/database", PID))

stats <- SaveData("trade",
                  "completed_tf_cpc_m49",
                  complete_trade_flow_cpc,
                  waitTimeout = 10800)

message(sprintf("[%s] Session/database write completed!", PID))

sprintf(
  "Module completed in %1.2f minutes.
  Values inserted: %s
  appended: %s
  ignored: %s
  discarded: %s",
  difftime(Sys.time(), startTime, units = "min"),
  stats[["inserted"]],
  stats[["appended"]],
  stats[["ignored"]],
  stats[["discarded"]]
)


### TO DO: FCL output
#complete_trade_flow_fcl <- complete_trade %>%
#  select_(~-cpc) %>%
#  transmute_(reportingCountryM49 = ~reporterM49,
#             partnerCountryM49 = ~partnerM49,
#             measuredElementTrade = ~flow,
#             measuredItemFS = ~fcl,
#             timePointYears = ~year,
#             flagObservationStatus = ~flagObservationStatus,
#             flagMethod = ~flagMethod,
#             qty = ~qty,
#             unit = ~fclunit,
#             value = ~value)

