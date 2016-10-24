set.seed(2507)

debughsfclmap <- TRUE
multicore <- FALSE

## If true, the reported values will be in $
## If false the reported values will be in k$
dollars <- FALSE


# ---- libs ----

#library(tradeproc)

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

# ---- swsdebug ----


if(CheckDebug()){
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/complete_tf_cpc/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

# ---- settings ----

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

# ---- datasets ----
## Data sets with hs->fcl map (from mdb files)
# and UNSD area codes (M49)

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
  mutate_(tocode = ~faoswsTrade::trailingDigits(fromcode,
                                                tocode,
                                                digit = 9))



# ---- data from Giorgio (from drive) ----

# ---- tradeload ----

#### Get list of agri codes ####
#agricodeslist <- paste0(shQuote(getAgriHSCodes(), "sh"), collapse=", ")

### Download TL data ####

# tldata <- getRawAgriTL(year, agricodeslist)

message(sprintf("[%s] Reading in Tariffline data", PID))
tldata <- ReadDatatable(paste0("ct_tariffline_unlogged_",year),
                        columns=c("rep", "tyear", "flow",
                                  "comm", "prt", "weight",
                                  "qty", "qunit", "tvalue",
                                  "chapter"),
                        where = "chapter IN ('01', '02', '03', '04', '05', '06', '07',
                        '08', '09', '10', '11', '12',  '13', '14',
                        '15', '16', '17', '18', '19', '20', '21',
                        '22', '23', '24', '33', '35', '38', '40',
                        '41', '42', '43', '50', '51', '52', '53')" ## Chapter provided by team B/C
                        )

# Removing duplicate values for which quantity & value & weight exist
# (in the process, removing redundant columns)
# Note: missing quantity|weight or value will be handled below by imputation
tldata_sws <- tldata %>%
  tbl_df() %>%
  select_(~-chapter) %>%
  # qty and weight seem to be always >0 or NA
  mutate_(no_quant = ~is.na(qty),
          no_weight = ~is.na(weight),
          no_tvalue = ~is.na(tvalue))

tldata <- bind_rows(
  tldata_sws %>%
    filter_(~(!no_quant & !no_tvalue & !no_weight)) %>%
    # XXX: in the original datatable there is a hsrep variable with different values
    group_by_(~tyear, ~rep, ~prt, ~flow, ~comm, ~qunit) %>%
    # na.rm is superfluous, but it hurts no one
    summarise_each_(funs(sum(., na.rm = TRUE)), vars = c("weight", "qty", "tvalue")) %>%
    ungroup(),
  tldata_sws %>%
    filter_(~(no_quant | no_tvalue | no_weight)) %>%
    select_(~-no_quant, ~-no_tvalue, ~-no_weight)
)

## comm (hs) code has to be digit
## This probably should be part of the faoswsEnsure
tldata <- tldata[grepl("^[[:digit:]]+$",tldata$comm),]

## Rename columns
tldata <- tldata %>%
  transmute_(reporter = ~as.integer(rep),
             partner = ~as.integer(prt),
             hs = ~comm,
             flow = ~as.integer(flow),
             year = ~as.character(tyear),
             value = ~tvalue,
             weight = ~weight,
             qty = ~qty,
             qunit = ~as.integer(qunit)) %>%
  mutate_(hs6 = ~stringr::str_sub(hs,1,6))

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
                                    "stat_regime")
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

esdata <- esdata %>%
  tbl_df() %>%
  transmute_(reporter = ~as.numeric(declarant),
             partner = ~as.numeric(partner),
             hs = ~product_nc,
             flow = ~as.integer(flow),
             year = ~as.character(str_sub(period,1,4)),
             value = ~as.numeric(value_1k_euro),
             weight = ~as.numeric(qty_ton),
             qty = ~as.numeric(sup_quantity)) %>%
  mutate_(hs6 = ~stringr::str_sub(hs,1,6))

# ---- geonom2fao ----

esdata <- data.table::as.data.table(esdata)
esdata[, `:=`(reporter = convertGeonom2FAO(reporter),
              partner = convertGeonom2FAO(partner))]
esdata <- esdata[partner != 252, ]
esdata <- tbl_df(esdata)

# ---- removing reporters for which we don't have mapping of commodities
esdata_not_area_in_fcl_mapping <- esdata %>%
  filter_(~!(reporter %in% unique(hsfclmap$area)))
esdata <- esdata %>%
  filter_(~reporter %in% unique(hsfclmap$area))

# ---- es_hs2fcl ----
message(sprintf("[%s] Convert Eurostat HS to FCL", PID))
esdata <- convertHS2FCL(esdata, hsfclmap, parallel = multicore)

# ---- remove non mapped fcls ----
## Non mapped FCL
esdata_fcl_not_mapped <- esdata %>%
  filter_(~is.na(fcl))

esdata <- esdata %>%
  filter_(~!(is.na(fcl)))
# ---- es_join_fclunits ----

esdata <- esdata %>%
  left_join(fclunits, by = "fcl")

## na fclunits has to be set up as mt (suggest by Claudia)
esdata$fclunit <- ifelse(is.na(esdata$fclunit),
                         "mt",
                         esdata$fclunit)

## Specific ES conversions: some FCL codes are reported in Eurostat
## with different supplementary units than those reported in FAOSTAT
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


# ---- tl_m49fao ----
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

tldata_not_area_in_fcl_mapping <- tldata %>%
  filter_(~!(reporter %in% unique(hsfclmap$area)))

tldata <- tldata %>%
  filter_(~reporter %in% unique(hsfclmap$area))

# ---- reexptoexp ----

# { "id": "1", "text": "Import" },
# { "id": "2", "text": "Export" },
# { "id": "4", "text": "re-Import" },
# { "id": "3", "text": "re-Export" }

tldata <- tldata %>%
  mutate_(flow = ~recode(flow, '4' = 1L, '3' = 2L))

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


# ---- tl_hs2fcl ----

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
# What units does FAO expects for given FCL codes

tldata <- tldata %>%
  left_join(fclunits, by = "fcl")

## na fclunits has to be set up as mt (suggest by Claudia)
tldata$fclunit <- ifelse(is.na(tldata$fclunit),
                         "mt",
                         tldata$fclunit)


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

if(NROW(fcl_spec_mt_conv) > 0){
  fcl_spec_mt_conv$convspec <- 0
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Cigarettes" &
                              fcl_spec_mt_conv$wco == "1000u"] <- .001
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Cigarettes" &
                              fcl_spec_mt_conv$wco == "u"] <- .000001
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Hen Eggs" &
                              fcl_spec_mt_conv$wco == "u"] <- .00006
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Hen Eggs" &
                              fcl_spec_mt_conv$wco == "2u"] <- .00012
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Hen Eggs" &
                              fcl_spec_mt_conv$wco == "12u"] <- .00072
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Hen Eggs" &
                              fcl_spec_mt_conv$wco == "1000u"] <- .06
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Cigars Cheroots" &
                              fcl_spec_mt_conv$wco == "u"] <- 0.000008
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Cigars Cheroots" &
                              fcl_spec_mt_conv$wco == "1000u"] <- 0.008
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Tobacco Products nes" &
                              fcl_spec_mt_conv$wco == "u"] <- 0.000008
  fcl_spec_mt_conv$convspec[fcl_spec_mt_conv$fcldesc == "Tobacco Products nes" &
                              fcl_spec_mt_conv$wco == "1000u"] <- 0.008
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
} else {
  tldata$qtyfcl = NA
}



##### No qty, but weight and target is mt: we take weight from there

tldata$qtyfcl <- ifelse(tldata$qty == 0 &
                          tldata$fclunit == "mt" &
                          is.na(tldata$qtyfcl) &
                          tldata$weight > 0,
                        tldata$weight,
                        tldata$qtyfcl)


######### Value from USD to thousands of USD
if (dollars){
  esdata$value <- esdata$value * 1000
} else { ## This means it is in k$
  tldata$value <- tldata$value / 1000
}


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
          ~fclunit,
          ~hs,
          weight = ~qtyfcl,
          ~qty,
          ~value)

tldata_mid = tldata

# Loading of notes/adjustments should be added here
esdata_old = esdata


message(sprintf("[%s] Applying Eurostat adjustments", PID))
esdata <- tbl_df(plyr::ldply(
  sort(unique(esdata$reporter)),
  function(x) {
    applyadj(x, year, as.data.frame(adjustments), esdata)
  },
  .progress = ifelse(!multicore && CheckDebug(), "text", "none"),
  .inform = FALSE,
  .parallel = multicore))

## Apply conversion EUR to USD
esdata$value <- esdata$value * as.numeric(EURconversionUSD %>%
                                            filter(Year == year) %>%
                                            select(ExchangeRate))

message(sprintf("[%s] Applying Tariffline adjustments", PID))

tldata <- tbl_df(plyr::ldply(
  sort(unique(tldata$reporter)),
  function(x) {
    applyadj(x, year, as.data.frame(adjustments), tldata)
  },
  .progress = ifelse(!multicore && CheckDebug(), "text", "none"),
  .inform = FALSE,
  .parallel = multicore))


# TODO Check quantity/weight
# The notes should save the results in weight

tradedata <- bind_rows(
  tldata %>%
    mutate_(hs = ~as.character(hs)) %>%
    select_(~year, ~reporter, ~partner, ~flow,
            ~fcl, ~fclunit, ~hs,
            qty = ~weight, ~value),
  esdata %>%
    mutate_(uniqqty = ~ifelse(fclunit == "mt", weight, qty)) %>%
    select_(~year, ~reporter, ~partner, ~flow,
            ~fcl, ~fclunit,~hs,
            qty = ~uniqqty, ~value)
)

# Detection of missing quantities and values

tradedata <- tradedata %>%
  mutate_(no_quant = ~qty == 0 | is.na(qty),  # There are no any NA qty records,
                                              # but may change later
          no_value = ~value == 0 | is.na(value))

# UV calculation

tradedata <- mutate_(tradedata,
                     uv = ~ifelse(no_quant | no_value, # Only 0 here.
                                                       # Should we care about NA?
                                  NA,
                                  value / qty))

# Outlier detection

tradedata <- tradedata %>%
  group_by_(~year, ~reporter, ~flow, ~hs) %>%
  mutate_(
    uv_reporter = ~median(uv, na.rm = T),
    outlier = ~uv %in% boxplot.stats(uv, coef = out_coef, do.conf = F)$out) %>%
  ungroup()

# Imputation of missings and outliers
tradedata <- tradedata %>%
  mutate_(qty = ~ifelse(no_quant | outlier,
                        value / uv_reporter,
                        qty),
          flagTrade = ~ifelse(no_quant | outlier, 1, 0))

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


# Non reporting countries
nonreporting <- unique(tradedata$partner)[!is.element(unique(tradedata$partner),
                                                      unique(tradedata$reporter))]

## Mirroring for non reporting countries
tradedatanonrep <- tradedata %>%
  filter_(~partner %in% nonreporting) %>%
  mutate_(partner_mirr = ~reporter,
          partner_mirrM49 = ~reporterM49,
          reporter = ~partner,
          reporterM49 = ~partnerM49,
          partner = ~partner_mirr,
          partnerM49 = ~partner_mirrM49,
          flow = ~recode(flow, '2' = 1, '1' = 2),
          ## Correction of CIF/FOB
          ## For now fixed at 12%
          ## but further analyses needed
          value = ~ifelse(flow == 1,
                          value*1.12,
                          value/1.12)) %>%
  select_(~-partner_mirr, ~-partner_mirrM49)

tradedata <- bind_rows(tradedata,
                       tradedatanonrep)

## Flag from numeric to letters
## TO DO (Marco): need to discuss how to treat flags
##                at the moment Status I and method e
##                for both imputed and mirrored
##                because applying 12% change in mirroring
complete_trade <- tradedata %>%
  mutate_(
    flagObservationStatus = ~ifelse((flagTrade > 0) &
                                      (fclunit != "$ value only"),"I",""),
    flagMethod = ~ifelse((flagTrade > 0) &
                           (fclunit != "$ value only"),"e",""))

# Output for the SWS

## Completed trade flow
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
             value = ~value)


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

