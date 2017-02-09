# Settings ####
set.seed(2507)
debughsfclmap <- TRUE

# Parallel backend will be used only if required packages
# are installed
# It will be switched to FALSE if packages are not available
multicore <- TRUE

## If true, the reported values will be in $
## If false the reported values will be in k$
dollars <- FALSE

## If TRUE, use adjustments (AKA "conversion notes")
use_adjustments <- FALSE

# Libraries ####
suppressPackageStartupMessages(library(data.table))
library(stringr)
library(scales)
library(tidyr)
library(futile.logger)
suppressPackageStartupMessages(library(dplyr, warn.conflicts = FALSE))
library(faosws)
library(faoswsUtil)
library(faoswsTrade)

flog.threshold(TRACE)

# Development (not SWS-inside) mode addons ####
if(faosws::CheckDebug()){
  localsettingspath <- "modules/complete_tf_cpc/sws.yml.example"
  SETTINGS <- faoswsModules::ReadSettings(localsettingspath)
  flog.debug("Local settings read from %s",
             localsettingspath)
  flog.debug("Local settings read:",
             SETTINGS,
             capture = TRUE)
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  faosws::GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                             token = SETTINGS[["token"]])
  # Fall-back R_SWS_SHARE_PATH var
  if(is.na(Sys.getenv("R_SWS_SHARE_PATH", unset = NA))) {
    flog.debug("R_SWS_SHARE_PATH system variable not found.")
    Sys.setenv("R_SWS_SHARE_PATH" = tempdir())
    flog.debug("R_SWS_SHARE_PATH now points to R temp directory %s",
               tempdir())
  }
}

flog.debug("User's computation parameters:",
           swsContext.computationParams, capture = TRUE)

# SWS user name ####
# Remove domain from username
SWS_USER <- regmatches(
  swsContext.username,
  regexpr("(?<=/).+$", swsContext.username, perl = TRUE))

stopifnot(!any(is.na(SWS_USER),
               SWS_USER == ""))


# Reporting directory ####

reportdir <- file.path(
  Sys.getenv("R_SWS_SHARE_PATH"),
  SWS_USER,
  paste0("complete_tf_cpc_",
         format(Sys.time(), "%Y%m%d%H%M%S%Z")))
stopifnot(!file.exists(reportdir))
dir.create(reportdir, recursive = TRUE)

flog.appender(appender.tee(file.path(reportdir,
                                      "report.txt")))

flog.info("SWS-session is run by user %s", SWS_USER)

flog.debug("R session environment: ",
           sessionInfo(), capture = TRUE)
if(!CheckDebug()){

  options(error = function(){
    dump.frames()
    save(last.dump,
         file = file.path(reportdir, "last.dump.RData"))
  })
}

PID <- Sys.getpid()

# Check that all packages are up to date ####
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

# Register CPU cores ####
if(multicore) {
  if(all(c("doParallel", "foreach") %in%
         rownames(installed.packages()))) {
    flog.debug("Multicore backend is available.")
    cpucores <- parallel::detectCores(all.tests = TRUE)
    flog.debug("CPU cores detected: %s.", cpucores)
    doParallel::registerDoParallel(cores = cpucores)
  } else {
    flog.debug("Multicore backend is not available.")
    multicore <- FALSE
  }
}


##+ swsdebug

## ## local data
## install.packages("//hqfile4/ess/Team_working_folder/A/SWS/faosws_0.8.2.9901.tar.gz",
##                  repos = NULL,
##                  type = "source")
## ## SWS data
## install.packages("faosws",
##                  repos = "http://hqlprsws1.hq.un.fao.org/fao-sws-cran/")

stopifnot(
  !is.null(swsContext.computationParams$year),
  !is.null(swsContext.computationParams$out_coef))

##' # Parameters
##' - `year`: year for processing.
year <- as.integer(swsContext.computationParams$year)
flog.info("Working year: %s", year)

##' - `out_coef`: coefficient for outlier detection, i.e., the `k` parameter in
##' the *Outlier Detection and Imputation* section.
# See coef argument in ?boxplot.stats
out_coef <- as.numeric(swsContext.computationParams$out_coef)
flog.info("Coefficient for outlier detection: %s", out_coef)

##' - `hs_chapters`: specific HS chapters that are downloaded (this parameter
##'   can not be set by the user as it is provided by Team B/C and harcoded).
##'   The HS chapters are the following:

hs_chapters <- c(1:24, 33, 35, 38, 40:43, 50:53)

flog.info("HS chapters to be selected:", hs_chapters,  capture = T)

startTime = Sys.time()

##' # Input Data
##'
##' ## Supplementary Datasets

##+ datasets

##' - `hsfclmap3`: Mapping between HS and FCL codes extracted from MDB files
##' used to archive information existing in the previous trade system
##' (Shark/Jellyfish). This mapping is provided by a separate package:
##' https://github.com/SWS-Methodology/hsfclmap

message(sprintf("[%s] Reading in hs-fcl mapping", PID))
flog.debug("Reading in hs-fcl mapping")
#data("hsfclmap3", package = "hsfclmap", envir = environment())
hsfclmap3 <- tbl_df(ReadDatatable("hsfclmap3"))

flog.info("Rows in HS->FCL mapping table: %s", nrow(hsfclmap3))

hsfclmap <- hsfclmap3 %>%
  filter_(~startyear <= year &
            endyear >= year)

stopifnot(nrow(hsfclmap) > 0)

flog.info("Rows in mapping table after filtering by year: %s",
          nrow(hsfclmap))

if(use_adjustments) {

  ##' - `adjustments`: Adjustment notes containing manually added conversion
  ##' factors to transform from non-standard units of measurement to standard
  ##' ones or to obtain quantities from traded values.

  ## Old precedure
  #data("adjustments", package = "hsfclmap", envir = environment())
  ## New procedure
  message(sprintf("[%s] Reading in adjustments", PID))

  adjustments <- tbl_df(ReadDatatable("adjustments"))
  colnames(adjustments) <- sapply(colnames(adjustments),
                                  function(x) gsub("adj_","",x))
  adj_cols_int <- c("year","flow","fcl","partner","reporter")
  adj_cols_dbl <- c("hs")
  adjustments <- adjustments %>%
    mutate_each_(funs(as.integer),adj_cols_int) %>%
    mutate_each_(funs(as.double),adj_cols_dbl)
}

##' - `unsdpartnersblocks`: UNSD Tariffline reporter and partner dimensions use
##' different list of geographic are codes. The partner dimesion is more
##' detailed than the reporter dimension. Since we can not split trade flows of
##' the reporter dimension, trade flows of the corresponding partner dimensions
##' have to be assigned the reporter dimension's geographic area code. For
##' example, the code 842 is used for the United States includes Virgin Islands
##' and Puerto Rico and thus the reported trade flows of those territories.
##' Analogous steps are taken for France, Italy, Norway, Switzerland and US
##' Minor Outlying Islands.

data("unsdpartnersblocks", package = "faoswsTrade", envir = environment())
#unsdpartnersblocks <- tbl_df(ReadDatatable("unsdpartnersblocks"))

##' - `fclunits`: For UNSD Tariffline units of measurement are converted to
##' meet FAO standards. According to FAO standard, all weights are reported in
##' tonnes, animals in heads or 1000 heads and for certain commodities,
##' only the value is provided.

data("fclunits", package = "faoswsTrade", envir = environment())
#fclunits <- tbl_df(ReadDatatable("fclunits"))

##' - `comtradeunits`: Translation of the `qunit` variable (supplementary
##' quantity units) in Tariffline data into intelligible unit of measurement,
##' which correspond to bthe standards of quantity recommended by the *World
##' Customs Organization* (WCO) (e.g., `qunit`=8 correspond to *kg*).
##' See: http://unstats.un.org/unsd/tradekb/Knowledgebase/UN-Comtrade-Reference-Tables

data("comtradeunits", package = "faoswsTrade", envir = environment())
#comtradeunits <- tbl_df(ReadDatatable("comtradeunits"))

##' - `EURconversionUSD`: Annual EUR/USD currency exchange rates table from SWS.

data("EURconversionUSD", package = "faoswsTrade", envir = environment())
#EURconversionUSD <- tbl_df(ReadDatatable("eur_conversion_usd"))

hs_chapters_str <-
  formatC(hs_chapters, width = 2, format = "d", flag = "0") %>%
  as.character %>%
  shQuote(type = "sh") %>%
  paste(collapse = ", ")

##' # Extract Eurostat Combined Nomenclature Data

##+ es-extract
#### Download ES data ####

##' 1. Download raw data from SWS, filtering by `hs_chapters`.

message(sprintf("[%s] Reading in Eurostat data", PID))
esdata <- ReadDatatable(paste0("ce_combinednomenclature_unlogged_",year),
                        columns = c("declarant", "partner",
                                    "product_nc", "flow",
                                    "period", "value_1k_euro",
                                    "qty_ton", "sup_quantity",
                                    "stat_regime"),
                        where = paste0("chapter IN (", hs_chapters_str, ")")
)

flog.info("Records in raw Eurostat data: %s", nrow(esdata))

##' 1. Remove non-numeric codes for reporters/partners/commodities.

## Declarant and partner numeric
## This probably should be part of the faoswsEnsure
esdata <- esdata[grepl("^[[:digit:]]+$",esdata$declarant),]

flog.info("Records after removing non-numeric reporter codes: %s", nrow(esdata))

esdata <- esdata[grepl("^[[:digit:]]+$",esdata$partner),]

flog.info("Records after removing non-numeric partner codes: %s", nrow(esdata))

## Removing TOTAL from product_nc column
esdata <- esdata[grepl("^[[:digit:]]+$",esdata$product_nc),]

flog.info("Records after removing non-numeric commodity codes: %s", nrow(esdata))

## Only regime 4 is relevant for Eurostat data
esdata <- esdata[esdata$stat_regime=="4",]

flog.info("Records after filtering by 4th stat regime: %s", nrow(esdata))

## Removing stat_regime as it is not needed anymore
esdata[,stat_regime:=NULL]

esdata <- tbl_df(esdata)

##' 1. Use standard (common) variable names (e.g., `declarant` becomes `reporter`).

esdata <- adaptTradeDataNames(tradedata = esdata, origin = "ES")

# Fiter out HS codes which don't participate in futher processing
# Such solution drops all HS codes shorter than 6 digits.

esdata <- filterHS6FAOinterest(esdata)

##' 1. Convert ES geonomenclature country/area codes to FAO codes.

##+ geonom2fao
esdata <- data.table::as.data.table(esdata)
esdata[, `:=`(reporter = convertGeonom2FAO(reporter),
              partner = convertGeonom2FAO(partner))]
esdata <- esdata[partner != 252, ]

flog.info("Records after removing partners' 252 code: %s", nrow(esdata))

esdata <- tbl_df(esdata)

##' 1. Remove reporters with area codes that are not included in MDB commodity
##' mapping area list.

##+ es-treat-unmapped
esdata_not_area_in_fcl_mapping <- esdata %>%
  filter_(~!(reporter %in% unique(hsfclmap$area)))

write.csv(esdata_not_area_in_fcl_mapping,
          file = file.path(reportdir,
                           "esdata_not_area_in_fcl_mapping.csv"))

esdata <- esdata %>%
  filter_(~reporter %in% unique(hsfclmap$area))

flog.info("Records after removing areas absent in HS->FCL map: %s",
          nrow(esdata))

## es_hs2fcl ####
message(sprintf("[%s] Convert Eurostat HS to FCL", PID))

##' 1. Map HS to FCL.

esdatalinks <- esdata %>% do(hsInRange(.$hs, .$reporter, .$flow,
                        hsfclmap,
                        parallel = multicore))

stopifnot(all(c("reporter", "flow", "hs") %in%
                colnames(esdatalinks)))
stopifnot(nrow(esdatalinks) > 0)

esdata <- esdata %>%
  left_join(esdatalinks, by = c("reporter", "flow", "hs"))

flog.info("Records after HS-FCL mapping: %s",
          nrow(esdata))

##' 1. Remove unmapped FCL codes.

## es remove non mapped fcls
esdata_fcl_not_mapped <- esdata %>%
  filter_(~is.na(fcl))

write.csv(esdata_fcl_not_mapped,
          file = file.path(reportdir,
                           "esdata_fcl_not_mapped.csv"))

esdata <- esdata %>%
  filter_(~!(is.na(fcl)))

flog.info("Records after removing non-mapped HS codes: %s",
          nrow(esdata))

##' 1. Add FCL units.

## es join fclunits
esdata <- addFCLunits(tradedata = esdata, fclunits = fclunits)

##' 1. Specific ES conversions: some FCL codes are reported in Eurostat
##' with different supplementary units than those reported in FAOSTAT,
##' thus a conversion is done.

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

##' # Extract UNSD Tariffline Data

##+ tradeload

##' 1. Download raw data from SWS, filtering by `hs_chapters`.

message(sprintf("[%s] Reading in Tariffline data", PID))
tldata <- ReadDatatable(paste0("ct_tariffline_unlogged_",year),
                        columns=c("rep", "tyear", "flow",
                                  "comm", "prt", "weight",
                                  "qty", "qunit", "tvalue",
                                  "chapter"),
                        where = paste0("chapter IN (", hs_chapters_str, ")")
                        )

##+ tl_m49fao
## Based on Excel file from UNSD (unsdpartners..)

##' 1. Remove non-numeric commodity codes.

##+ tl-force-numeric-comm

# This probably should be part of the faoswsEnsure
tldata <- tldata[grepl("^[[:digit:]]+$",tldata$comm),]

tldata <- tbl_df(tldata)

##+ tl-aggregate-multiple-rows

##' 1. Identical combinations of reporter / partner / commodity / flow / year / qunit
##' are aggregated.

tldata <- preAggregateMultipleTLRows(tldata)

##' 1. Use standard (common) variable names (e.g., `rep` becomes `reporter`).

tldata <- adaptTradeDataNames(tradedata = tldata, origin = "TL")

tldata <- filterHS6FAOinterest(tldata)

##' 1. Tariffline M49 codes (which are different from official M49)
##' are converted in FAO country codes using a specific convertion
##' table provided by Team ENV.

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


##+ drop_es_from_tl

##' 1. European countries are removed (will be replaced by ES data).

# They will be replaced by ES data

tldata <- tldata %>%
  anti_join(esdata %>%
              select_(~reporter) %>%
              distinct(),
            by = "reporter")

##+ drop_reps_not_in_mdb

##' 1. Area codes not mapping to any FAO country code are removed.

# We drop reporters what are absent in MDB hsfcl map
# because in any case we can proceed their data

tldata_not_area_in_fcl_mapping <- tldata %>%
  filter_(~!(reporter %in% unique(hsfclmap$area)))

tldata <- tldata %>%
  filter_(~reporter %in% unique(hsfclmap$area))


##+ reexptoexp

##' 1. Re-imports become imports and re-exports become exports.

# { "id": "1", "text": "Import" },
# { "id": "2", "text": "Export" },
# { "id": "4", "text": "re-Import" },
# { "id": "3", "text": "re-Export" }

tldata <- tldata %>%
  mutate_(flow = ~recode(flow, '4' = 1L, '3' = 2L))

##' 1. Map HS to FCL.

##+ tl_hs2fcl

tldatalinks <- tldata %>%
  do(hsInRange(.$hs, .$reporter, .$flow,
               hsfclmap,
               parallel = multicore))

tldata <- tldata %>%
  left_join(tldatalinks, by = c("reporter", "flow", "hs"))

##' 1. Remove unmapped FCL codes.

## Non mapped FCL
tldata_fcl_not_mapped <- tldata %>%
  filter_(~is.na(fcl))

tldata <- tldata %>%
  filter_(~!(is.na(fcl)))

#############Units of measurment in TL ####

##' 1. Add FCL units.

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

##' 1. General TL conversions: some FCL codes are reported in Tariffline
##' with different units than those reported in FAOSTAT, thus a conversion
##' is done.

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

##' 1. Specific TL conversions: some commodities need a specific conversion.

#### Commodity specific conversion

fcl_spec_mt_conv <- tldata %>%
  filter_(~fclunit == "mt" & is.na(weight) & conv == 0) %>%
  select_(~fcl, ~wco) %>%
  distinct

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
    left_join(fcl_spec_mt_conv,
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


##' 1. If the `quantity` variable is not reported, but the `weight` variable is and
##' the final unit of measurement is tonnes the `weight` is used as `quantity`

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

##' 1. Aggregate UNSD Tariffline Data to FCL.

##+ tl_aggregate

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

##' # Combine Trade Data Sources

if (use_adjustments) {
##' 1. Application of "adjustment notes" to both ES and TL data.

# TODO Check quantity/weight
# The notes should save the results in weight

  esdata <- useAdjustments(tradedata = esdata, year = year, PID = PID,
                           adjustments = adjustments, parallel = multicore)

  tldata <- useAdjustments(tradedata = tldata, year = year,
                           adjustments = adjustments, parallel = multicore)
}

##+ es_convcur

##' 1. Convert currency of monetary values from EUR to USD using the
##' `EURconversionUSD` table.

esdata$value <- esdata$value * as.numeric(EURconversionUSD %>%
                                            filter(Year == year) %>%
                                            select(ExchangeRate))

##' 1. Combine UNSD Tariffline and Eurostat Combined Nomenclature data sources
##' to single data set.
##'     - TL: assign `weight` to `qty`
##'     - ES: assign `weight` to `qty` if `fclunit` is equal to `mt`, else keep `qty`

##+ combine_es_tl

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

##' # Outlier Detection and Imputation

##+ calculate_median_uv

tradedata <- tradedata %>%
  mutate_(no_quant = ~near(qty, 0) | is.na(qty),
          no_value = ~near(value, 0) | is.na(value))

##' 1. Unit values are calculated for each observation at the HS level as ratio
##' of monetary value over quantity `value / qty`.

tradedata <- mutate_(tradedata,
                     uv = ~ifelse(no_quant | no_value, NA, value / qty))

## Round UV in order to avoid floating point number problems (see issue #54)
tradedata$uv <- round(tradedata$uv, 10)

##+ boxplot_uv

##' 1. Outlier detection by using the logarithm of the unit value.

tradedata <- detectOutliers(tradedata = tradedata, method = "boxplot",
                            parameters = list(out_coef=out_coef))

##+ impute_qty_uv

##' 1. Imputation of missing quantities and quantities categorized as outliers by
##' applying the method presented in the *Outlier Detection and Imputation* section.
##' The `flagTrade` variable is given a value of 1 if an imputation was performed.

## These flags are also assigned to monetary values. This may need to be revised
## (monetary values are not supposed to be modified).

tradedata <- doImputation(tradedata = tradedata)

##' 1. Aggregate values and quantities by FCL codes.

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

##' 1. Map FCL codes to CPC.

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

##' 1. Map FAO area codes to M49.

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

##+ mirror_estimation

##' # Mirror Trade Estimation

##' 1. Obtain list of non-reporting countries as difference between the list of
##' reporter countries and the list of partner countries.

nonreporting <- unique(tradedata$partner)[!is.element(unique(tradedata$partner),
                                                      unique(tradedata$reporter))]

##' 1. Swap the reporter and partner dimensions: the value previously appearing
##' as reporter country code becomes the partner country code (and vice versa).

##' 1. Invert the flow direction: an import becomes an export (and vice versa).

##' 1. Calculate monetary mirror value by adding (removing) a 12% mark-up on
##' imports (exports) to account for the difference between CIF and FOB prices.

## Mirroring for non reporting countries
tradedata <- mirrorNonReporters(tradedata = tradedata,
                                nonreporters = nonreporting)
##' ## Flag management

##' **Note**: work on this section is currently in progress.
##'
##' - observationStatus:
##'     - Reporting countries:
##'         - `X` if `flagTrade` is zero (i.e., no imputation) and FCL unit != "$ value only"
##'         - `I` if `flagTrade` is non-zero (i.e., imputation) and FCL unit != "$ value only"
##'   - Non-reporting countries: `E`
##'
##' - flagMethod:
##'     - Reporting countries:
##'         - `<BLANK>` if `flagTrade` is zero (i.e., no imputation) and FCL unit != "$ value only"
##'         - `e` if `flagTrade` is non-zero (i.e., imputation) and FCL unit != "$ value only"
##'   - Non-reporting countries: `e`

##+ sws_flag

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

##+ completed_trade_flow

##' # Output for SWS

##' 1. Filter observations with FCL code `1181` (bees).

##' 1. Filter observations with missing CPC codes.

##' 1. Rename dimensions to comply with SWS standard, e.g., `geographicAreaM49Reporter`

##' 1. Calculate unit value (US$ per quantity unit) at CPC level if the quantity is larger than zero

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


##' 1. Transform dataset separating monetary values, quantities and unit values
##' in different rows.

##' 1. Convert monetary values, quantities and unit values to corresponding SWS
##' element codes. For example, a quantity import measured in metric tons is
##' assigned `5610`.

##+ convert_element

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
  select_(~-flow,~-unit)

##' 1. Overwrite **flagMethod** for mirrored quantities: `e` becomes `c`

##+ overwrite_mirror_method_flag

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


##' 1. Add **flagMethod** `i` to unit values

##+ add_uv_method_flag

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

