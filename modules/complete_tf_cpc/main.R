##' ---
##' title: "Appendix: `complete_tf_cpc` module"
##' author:
##'   - Marco Garieri
##'   - Alexander Matrunich
##'   - Christian A. Mongeau Ospina
##'   - Bo Werth\
##'
##'     Food and Agriculture Organization of the United Nations
##' date: "`r format(Sys.time(), '%e %B %Y')`"
##' output:
##'    pdf_document
##' ---

##' This document gives a faithful step-by-step sequence of the operations
##' performed in the `complete_tf_cpc` module. For a narrative version of
##' the module's approach, please see its main document.

# Stop if required parameters were not set
stopifnot(!is.null(swsContext.computationParams$out_coef))
stopifnot(!is.null(swsContext.computationParams$year))

##+ setup, include=FALSE
knitr::opts_chunk$set(echo = FALSE, eval = FALSE)

startTime = Sys.time()

# Libraries ####
suppressPackageStartupMessages(library(data.table))
library(stringr)
library(magrittr)
library(scales)
library(tidyr, warn.conflicts = FALSE)
library(futile.logger)
suppressPackageStartupMessages(library(dplyr, warn.conflicts = FALSE))
library(faosws)
library(faoswsUtil)
library(faoswsTrade)
library(faoswsFlag)
library(bit64)

# Always source files in R/ (useful for local runs)
sapply(dir("R", full.names = TRUE), source)

##+ init

## **Flow chart:**
##
## ![Aggregate complete_tf to total_trade](assets/diagram/trade_3.png?raw=true "livestock Flow")

# Settings ####

# Should we do just pre-processing reports?
# If TRUE no auxiliary files will be read (unless they are required)
# and the module will stop as soon as reports on raw data are done.
only_pre_process <- FALSE

# Package build ID (it is included into report directory name)
build_id <- "master"

# Should we stop after HS-FCL mapping?
stop_after_mapping <- FALSE

set.seed(2507)

# Size for sampling. Set NULL if no sampling is required.
samplesize <- NULL

# Logging level. There are following levels (`trace` shows everything in log):
# trace, debug, info, warn, error, fatal

# Additional logger for technical data
futile.logger::flog.logger("dev", "TRACE")
futile.logger::flog.threshold("TRACE", name = "dev")

# Parallel backend will be used only if required packages are installed.
# It will be switched to FALSE if packages are not available.
multicore <- TRUE

## If TRUE, reported values will be in $, if FALSE in k$
dollars <- FALSE

## If TRUE, use adjustments (AKA "conversion notes"), if FALSE don't use them
use_adjustments <- FALSE

# If TRUE, impute outliers, if FALSE no imputation occurs
detect_outliers <- FALSE

# Print general log to console
general_log2console <- FALSE

# Save current options (will be reset at the end)
old_options <- options()

dev_sws_set_file <- "modules/complete_tf_cpc/sws.yml"

# Switch off dplyr's progress bars globally
options(dplyr.show_progress = FALSE)

# max.print in RStudio is too small
options(max.print = 99999L, scipen = 999)

# Development (SWS-outside) mode addons ####
if (faosws::CheckDebug()){
  set_sws_dev_settings(dev_sws_set_file)
} else {
  # In order to have all columns aligned. Issue #119
  options(width = 1000L)

  # Remove domain from username
  USER <- regmatches(
    swsContext.username,
    regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
  )

  options(error = function(){
    dump.frames()
    filename <- file.path(Sys.getenv("R_SWS_SHARE_PATH"),
                          USER,
                          "complete_tf_cpc")
    dir.create(filename, showWarnings = FALSE, recursive = TRUE)
    save(last.dump, file = file.path(filename, "last.dump.RData"))
  })
}

stopifnot(!any(is.na(USER), USER == ""))

# Read SWS module run parameters ####

##' # Parameters

# Below the calls to `exists()` is useful if it the parameters
# were set in an interactive session

##' - `year`: year for processing.
if (!exists('year', inherits = FALSE)) {
  year <- as.integer(swsContext.computationParams$year)
}
flog.info("Year: %s", year)

##' - `out_coef`: coefficient for outlier detection, i.e., the `k` parameter in
##' the *Outlier Detection and Imputation* section.
# See coef argument in ?boxplot.stats
if (!exists('out_coef', inherits = FALSE)) {
  out_coef <- as.numeric(swsContext.computationParams$out_coef)
}
flog.info("Coefficient for outlier detection: %s", out_coef)

reportdir <- reportdirectory(USER, year, build_id, browsedir = CheckDebug())
report_txt <- file.path(reportdir, "report.txt")
dev_log <- file.path(reportdir, "development.log")

# Send general log messages
if (general_log2console) {
  # to console and a file
  flog.appender(appender.tee(report_txt))
} else {
  # to a file only
  flog.appender(appender.file(report_txt))
}

# Send technical log messages to a file and console
flog.appender(appender.tee(dev_log), name = "dev")

flog.info("SWS-session is run by user %s", USER, name = "dev")

flog.debug("User's computation parameters:",
           swsContext.computationParams, capture = TRUE,
           name = "dev")

flog.info("R session environment: ",
           sessionInfo(), capture = TRUE, name = "dev")

PID <- Sys.getpid()

# Check that all packages are up to date ####

check_versions(c('faoswsUtil', 'faoswsTrade', 'dplyr'),
               c('0.2.11',     '0.1.1',       '0.5.0'))

# Register CPU cores ####
if (multicore) multicore <- register_cpu_cores()

##+ swsdebug

## ## local data
## install.packages("//hqfile4/ess/Team_working_folder/A/SWS/faosws_0.8.2.9901.tar.gz",
##                  repos = NULL,
##                  type = "source")
## ## SWS data
## install.packages("faosws",
##                  repos = "http://hqlprsws1.hq.un.fao.org/fao-sws-cran/")

##+ hschapters, eval = TRUE

hs_chapters <- c(1:24, 33, 35, 38, 40:41, 43, 50:53) %>%
  formatC(width = 2, format = "d", flag = "0") %>%
  as.character %>%
  shQuote(type = "sh") %>%
  paste(collapse = ", ")

##'   - `hs_chapters`: can not be set by the user as it is provided by Team B/C and harcoded).
##'   The HS chapters are the following:

##'     `r paste(formatC(hs_chapters, width = 2, format = "d", flag = "0"), collapse = ' ')`

##'

flog.info("HS chapters to be selected:", hs_chapters,  capture = T)

##' # Download helper tables

# If running the whole module (only_pre_process = FALSE) the various
# helper files will be read before everything else and a check will
# be done so that if there is any issue in reading any of these tables
# the module should exit immediately (i.e., it makes no sense to read
# the whole trade data, do computations, etc. if the module is going
# to fail because a table could not be read at some later step).
# They are in ordered based on increasing time required for reading.

# TODO: there are basic checks on the tables (mainly that they have
# data), but more detailed checks should be needed (see #132)

if (!only_pre_process) {
  flog.trace("[%s] Reading in comtradeunits datatable", PID, name = "dev")
  comtradeunits <- ReadDatatable('comtradeunits')
  stopifnot(nrow(comtradeunits) > 0)

  flog.trace("[%s] Reading in EURconversionUSD datatable", PID, name = "dev")
  EURconversionUSD <- ReadDatatable('eur_conversion_usd')
  stopifnot(nrow(EURconversionUSD) > 0)
  stopifnot(year %in% EURconversionUSD$eusd_year)

  flog.trace("[%s] Reading in fclunits datatable", PID, name = "dev")
  fclunits <- ReadDatatable('fclunits')
  stopifnot(nrow(fclunits) > 0)

  flog.trace("[%s] Reading in fcl_codes datatable", PID, name = "dev")
  fcl_codes <- ReadDatatable('fcl_2_cpc')$fcl
  stopifnot(length(fcl_codes) > 0)

  flog.trace("[%s] Reading in hs6standard datatable", PID, name = "dev")
  hs6standard <- ReadDatatable('standard_hs12_6digit')
  stopifnot(nrow(hs6standard) > 0)

  flog.trace("[%s] Reading in add_map datatable", PID, name = "dev")
  add_map <- ReadDatatable('hsfclmap4')
  stopifnot(nrow(add_map) > 0)

  if (use_adjustments) {
    flog.trace("[%s] Reading in adjustments datatable", PID, name = "dev")
    adjustments <- ReadDatatable('adjustments')
    stopifnot(nrow(adjustments) > 0)
  }

  flog.trace("[%s] Reading in hsfclmap3 datatable", PID, name = "dev")
  hsfclmap3 <- ReadDatatable('hsfclmap5')
  stopifnot(nrow(hsfclmap3) > 0)
}

# Required in pre-processing
flog.trace("[%s] Reading in unsdpartnersblocks datatable", PID, name = "dev")
unsdpartnersblocks <- ReadDatatable('unsdpartnersblocks')
stopifnot(nrow(unsdpartnersblocks) > 0)

##' # Download raw data and basic operations

##' 1. Download Eurostat data (ES).

flog.trace("[%s] Reading in Eurostat data", PID, name = "dev")

esdata <- ReadDatatable(
  paste0("ce_combinednomenclature_unlogged_", year),
  columns = c(
    "period",
    "declarant",
    "partner",
    "flow",
    "product_nc",
    "value_1k_euro",
    "qty_ton",
    "sup_quantity",
    "stat_regime"
  ),
  where = paste0("chapter IN (", hs_chapters, ")")
) %>% tbl_df()

stopifnot(nrow(esdata) > 0)

# Sample, if required

if (!is.null(samplesize)) {
  esdata <- sample_n(esdata, samplesize)
  warning(sprintf("Eurostat data was sampled with size %d", samplesize))
}

flog.info("Raw Eurostat data preview:", rprt_glimpse0(esdata), capture = TRUE)

##' 1. Download Tariff line data (TL).

flog.trace("[%s] Reading in Tariffline data", PID, name = "dev")

tldata <- ReadDatatable(
  paste0("ct_tariffline_unlogged_", year),
  columns = c(
    "tyear",
    "rep",
    "prt",
    "flow",
    "comm",
    "tvalue",
    "weight",
    "qty",
    "qunit",
    "chapter"
  ),
  where = paste0("chapter IN (", hs_chapters, ")")
) %>% tbl_df()

stopifnot(nrow(tldata) > 0)

# Sample, if required

if (!is.null(samplesize)) {
  tldata <- sample_n(tldata, samplesize)
  warning(sprintf("Tariffline data was sampled with size %d", samplesize))
}

flog.info("Raw Tariffline data preview:", rprt_glimpse0(tldata), capture = TRUE)

##' 1. Keep only `stat_regime` = 4 in ES.

##' 1. Remove European-aggregated data (i.e., totals) from ES.

## Only regime 4 is relevant for Eurostat data
esdata <- esdata %>%
  filter_(~stat_regime == "4") %>%
  ## Removing stat_regime as it is not needed anymore
  select_(~-stat_regime) %>%
  # Remove totals, 1010 = 'European Union', 1011 = 'Extra-European Union', see
  # http://ec.europa.eu/eurostat/documents/3859598/5889816/KS-BM-05-002-EN.PDF
  filter_(~!(declarant == 'EU' | partner %in% c('1010', '1011')))

flog.info("Records after removing 4th regime and EU totals: %s", nrow(esdata))

##' 1. Use standard (common) variable names (e.g., `declarant` becomes `reporter`) in ES and TL.

esdata <- adaptTradeDataNames(esdata)
tldata <- adaptTradeDataNames(tldata)

##' 1. Remove non numeric reporters / partners / hs codes from ES and TL.

esdata <- removeNonNumeric(esdata)
tldata <- removeNonNumeric(tldata)

##' 1. Use standard (common) variable types in ES and TL.

esdata <- adaptTradeDataTypes(esdata)
tldata <- adaptTradeDataTypes(tldata)

##' 1. Filter HS codes of interest, i.e., codes that do not
##' participate in further processing. Such solution drops,
##' e.g., all HS codes shorter than 6 digits.

esdata <- filterHS6FAOinterest(esdata)
tldata <- filterHS6FAOinterest(tldata)

##' 1. Convert ES geonomenclature country/area codes to FAO codes.

##+ geonom2fao
esdata <- esdata %>%
  mutate(
    reporter = convertGeonom2FAO(reporter),
    partner  = convertGeonom2FAO(partner)
  ) %>%
  # XXX issue 147
  filter(!is.na(partner))


# M49 to FAO area list ####

##' 1. TL M49 codes (which are different from official M49) are
##' converted in FAO country codes using a specific convertion
##' table provided by Team ENV. See below for the description
##' of the `unsdpartnersblocks` table.

flog.trace("TL: converting M49 to FAO area list", name = "dev")

# XXX This is also read below when "helper" tables are loaded.
unsdpartnersblocks <- tbl_df(unsdpartnersblocks)

tldata <- tldata %>%
  left_join(
    unsdpartnersblocks %>%
      select_(
        wholepartner = ~unsdpb_rtcode,
        part         = ~unsdpb_formula
      ) %>%
      mutate(
        wholepartner = as.numeric(wholepartner),
        part         = as.numeric(part)
      ) %>%
      # Exclude EU grouping and old countries
      filter_(
        ~wholepartner %in% c(251, 381, 579, 581, 757, 842)
      ),
    by = c("partner" = "part")
  ) %>%
  mutate_(
    partner  = ~ifelse(is.na(wholepartner), partner, wholepartner),
    m49rep   = ~reporter,
    m49par   = ~partner,
    # Conversion from Comtrade M49 to FAO area list
    reporter = ~as.integer(convertComtradeM49ToFAO(m49rep)),
    partner  = ~as.integer(convertComtradeM49ToFAO(m49par))
  )

##' 1. Remove invalid repoters (i.e., keep contries/areas that
##' existed in the year considered).

tldata <- removeInvalidReporters(tldata)

##' 1. Remove ES repoters from TL.

flog.trace("TL: dropping reporters already found in Eurostat data", name = "dev")
# They will be replaced by ES data
tldata <- tldata %>%
  anti_join(
    esdata %>%
      select_(~reporter) %>%
      distinct(),
    by = "reporter"
  )

# XXX create all reporters

tldata_rep_table <- tldata %>%
  select(reporter, flow) %>%
  distinct() %>%
  mutate(name = faoAreaName(reporter, "fao"))

rprt_writetable(tldata_rep_table, subdir = 'preproc')

# XXX this is a duplication: a function should be created.
to_mirror_raw <- bind_rows(
    esdata %>%
      select(year, reporter, partner, flow),
    tldata %>%
      filter(!(reporter %in% unique(esdata$reporter))) %>%
      select(year, reporter, partner, flow)
  ) %>%
  mutate(flow = recode(flow, '4' = 1L, '3' = 2L)) %>%
  flowsToMirror(names = TRUE)

rprt_writetable(to_mirror_raw, 'flows', subdir = 'preproc')

if (only_pre_process) stop("Stop after reports on raw data")

##' # Loading of help datasets

##' - `hsfclmap`: Mapping between HS and FCL codes extracted from MDB files
##' used to archive information existing in the previous trade system
##' (Shark/Jellyfish). This mapping table contains (identifier: `hsfclmap5`)
##' also some "corrections" to the original mapping found in the MDB files.
##' These are contained in the `correction_*` variables (e.g.,
##' `corrections_fcl`), and if for a given HS range one or more of these
##' variables are non-missing they will replace the original corresponding
##' variable (e.g., if `corresponding_fcl` is non-missing, it will replace
##' `fcl`). Missing HS to FCL links in the MDB files are mapped by Team B/C
##' and stored in a table (identifier: `hsfclmap4`) that will extend the
##' original mapping table. *[Note: for reference, the actual name of the
##' initial mapping table is `hsfclmap3`; the naming convention of these
##' tables should probably be made more logical or, at least, more easily
##' identifiable.]* The resulting mapping table gets subsetted with the
##' condition that the`startyear` and `endyear` of the HS to FCL links
##' should satisfy the condition: $startyear <= year <= endyear$.

flog.debug("[%s] Reading in hs-fcl mapping", PID, name = "dev")
#data("hsfclmap3", package = "hsfclmap", envir = environment())
# XXX Notice that it is pulling now v5
hsfclmap3 <- tbl_df(hsfclmap3) %>%
  # FCL, startyear, endyear codes can be overwritten by corrections
  mutate(
    fcl       = ifelse(!is.na(correction_fcl), correction_fcl, fcl),
    startyear = ifelse(!is.na(correction_startyear), correction_startyear, startyear),
    endyear   = ifelse(!is.na(correction_endyear), correction_endyear, endyear)
  ) %>%
  select(-starts_with('correction'))

# Extend endyear to 2050
hsfclmap3_extend <- hsfclmap3 %>%
  group_by(area, flow, fromcode, tocode) %>%
  summarise(maxy = max(endyear)) %>%
  mutate(extend = ifelse(maxy < 2050, TRUE, FALSE)) %>%
  ungroup()

hsfclmap3 <-
  left_join(
    hsfclmap3,
    hsfclmap3_extend,
    by = c('area', 'flow', 'fromcode', 'tocode')
  ) %>%
  mutate(endyear = ifelse(endyear == maxy & extend, 2050, endyear)) %>%
  select(-maxy, -extend)
# / Extend endyear to 2050

# ADD UNMAPPED CODES


fcl_codes <- as.numeric(fcl_codes)

##' - `hsfclmap4`: Additional mapping between HS and FCL codes (extends `hsfclmap`).

add_map <- tbl_df(add_map) %>%
  filter(!is.na(year), !is.na(reporter_fao), !is.na(hs)) %>%
  mutate(
    hs = ifelse(
           hs_chap < 10 & stringr::str_sub(hs, 1, 1) != '0',
           paste0('0', formatC(hs, format = 'fg')),
           formatC(hs, format = 'fg')
         ),
    hs = stringr::str_replace_all(hs, ' ', '')
  ) %>%
  arrange(reporter_fao, flow, hs, year)


## XXX change some FCL codes that are not valid
add_map <- add_map %>%
  mutate(fcl = ifelse(fcl == 389, 390, fcl)) %>%
  mutate(fcl = ifelse(fcl == 654, 653, fcl))

# Check that all FCL codes are valid

fcl_diff <- setdiff(unique(add_map$fcl), fcl_codes)

fcl_diff <- fcl_diff[!is.na(fcl_diff)]

fcl_diff <- setdiff(fcl_diff, 0)

if (length(fcl_diff) > 0) {
    warning(paste('Invalid FCL codes:', paste(fcl_diff, collapse = ', ')))
}

# Check that years are in a valid range

if (min(add_map$year) < 2000) {
  warning('The minimum year should not be lower than 2000.')
}

if (max(add_map$year) > as.numeric(format(Sys.Date(), '%Y'))) {
  warning('The maximum year should not be greater than the current year.')
}

# Check that there are no duplicate codes

tmp <- add_map %>%
  count(reporter_fao, year, flow, hs) %>%
  filter(n > 1)

if (nrow(tmp) > 0) {
  warning('Removing duplicate HS codes by reporter/year/flow.')
  
  # XXX
  add_map <- add_map %>%
    group_by(reporter_fao, year, flow, hs) %>%
    mutate(n = n(), i = 1:n(), hs_ext_perc = sum(!is.na(hs_extend))/n()) %>%
    ungroup() %>%
    # Prefer cases where hs_extend is available
    filter(hs_ext_perc == 0 | (hs_ext_perc > 0 & !is.na(hs_extend) & n == 1L)) %>%
    select(-n, -i, -hs_ext_perc)
}

# Raise warning if countries were NOT in mapping.

if (length(setdiff(unique(add_map$reporter_fao), hsfclmap3$area)) > 0) {
  warning('Some countries were not in the original mapping.')
}

add_map <- add_map %>%
  mutate(
    startyear = year,
    endyear = 2050L,
    fromcode = hs,
    tocode = hs,
    recordnumb = NA_integer_
  ) %>%
  select(
    area = reporter_fao,
    flow,
    fromcode,
    tocode,
    fcl,
    startyear,
    endyear,
    recordnumb,
    details,
    tl_description
  )

max_record <- max(hsfclmap3$recordnumb)

add_map$recordnumb <- (max_record+1):(max_record+nrow(add_map))

add_map <- add_map %>%
  select(-details, -tl_description) %>%
  mutate(
    fcl      = as.numeric(fcl),
    fromcode = gsub(' ', '', fromcode),
    tocode   = gsub(' ', '', tocode)
  )

hsfclmap3 <- bind_rows(add_map, hsfclmap3) %>%
  mutate(
    startyear = as.integer(startyear),
    endyear   = as.integer(endyear)
  )

# / ADD UNMAPPED CODES

flog.info("HS->FCL mapping table preview:",
          rprt_glimpse0(hsfclmap3), capture = TRUE)

rprt(hsfclmap3, "hsfclmap", year)

hsfclmap <- hsfclmap3 %>%
  filter_(~startyear <= year & endyear >= year) %>%
  select_(~-startyear, ~-endyear)

# Workaround issue #123
hsfclmap <- hsfclmap %>%
  mutate_at(vars(ends_with("code")), funs(num = as.numeric)) %>%
  mutate_(fromgtto = ~fromcode_num > tocode_num) %>%
  select(-ends_with("code_num"))

from_gt_to <- hsfclmap$recordnumb[hsfclmap$fromgtto]

if (length(from_gt_to) > 0)
  flog.warn(paste0("In following records of hsfclmap fromcode greater than tocode: ",
                 paste0(from_gt_to, collapse = ", ")))

hsfclmap <- hsfclmap %>%
  filter_(~!fromgtto) %>%
  select_(~-fromgtto)

stopifnot(nrow(hsfclmap) > 0)

flog.info("Rows in mapping table after filtering by year: %s", nrow(hsfclmap))

# HS6standard: will be used as last resort for mapping

hs6standard <- hs6standard %>%
  group_by(hs2012_code) %>%
  mutate(n = n()) %>%
  ungroup() %>%
  filter(n == 1) %>%
  mutate(hs6 = as.integer(hs2012_code)) %>%
  select(hs6, hs2012_code, faostat_code)


if (use_adjustments) {

  ##' - `adjustments`: Adjustment notes containing manually added conversion
  ##' factors to transform from non-standard units of measurement to standard
  ##' ones or to obtain quantities from traded values.

  ## Old precedure
  #data("adjustments", package = "hsfclmap", envir = environment())
  ## New procedure
  message(sprintf("[%s] Reading in adjustments", PID))

  adjustments <- tbl_df(adjustments)
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

#data("unsdpartnersblocks", package = "faoswsTrade", envir = environment())
unsdpartnersblocks <- unsdpartnersblocks # already dowloaded

##' - `fclunits`: For UNSD Tariffline units of measurement are converted to
##' meet FAO standards. According to FAO standard, all weights are reported in
##' tonnes, animals in heads or 1000 heads and for certain commodities,
##' only the value is provided.

#data("fclunits", package = "faoswsTrade", envir = environment())
fclunits <- tbl_df(fclunits) %>%
  rename(fcl = fcu_fcl, fclunit = fcu_fclunit) %>%
  mutate(fcl = as.integer(fcl))

##' - `comtradeunits`: Translation of the `qunit` variable (supplementary
##' quantity units) in Tariffline data into intelligible unit of measurement,
##' which correspond to bthe standards of quantity recommended by the *World
##' Customs Organization* (WCO) (e.g., `qunit`=8 correspond to *kg*).
##' See: http://unstats.un.org/unsd/tradekb/Knowledgebase/UN-Comtrade-Reference-Tables

#data("comtradeunits", package = "faoswsTrade", envir = environment())
comtradeunits <- tbl_df(comtradeunits) %>%
  rename(qunit = ctu_qunit, wco = ctu_wco, desc = ctu_desc) %>%
  mutate(qunit = as.integer(qunit))

##' - `EURconversionUSD`: Annual EUR/USD currency exchange rates table from SWS.

EURconversionUSD <- EURconversionUSD # already downloaded

##' # Generate HS to FCL map at HS6 level

# hs6fclmap ####

flog.trace("Extraction of HS6 mapping table", name = "dev")

##' 1. Universal (all years) HS6 mapping table.

flog.trace("Universal (all years) HS6 mapping table", name = "dev")

hs6fclmap_full <- extract_hs6fclmap(hsfclmap3, parallel = multicore)

##' 1. Current year specific HS6 mapping table.

flog.trace("Current year specific HS6 mapping table", name = "dev")

hs6fclmap_year <- extract_hs6fclmap(hsfclmap, parallel = multicore)

hs6fclmap <- bind_rows(hs6fclmap_full, hs6fclmap_year) %>%
  filter_(~fcl_links == 1L) %>%
  distinct()

rprt(hs6fclmap, "hs6fclmap")

##' # Specific operations on Eurostat data

##' 1. Add variables that will contain flags. (Note: flags are set in various
##' steps in the code. Please, refer to the "Flag Management in the Trade module"
##' document.)

esdata <- generateFlagVars(esdata)

##' 1. Generate Observation Status "X" flag and Method "h" flag.

esdata <- esdata %>%
  setFlag3(!is.na(value),  type = 'status', flag = 'X', variable = 'value') %>%
  setFlag3(!is.na(weight), type = 'status', flag = 'X', variable = 'weight') %>%
  setFlag3(!is.na(qty),    type = 'status', flag = 'X', variable = 'quantity') %>%
  setFlag3(!is.na(value),  type = 'method', flag = 'h', variable = 'value') %>%
  setFlag3(!is.na(weight), type = 'method', flag = 'h', variable = 'weight') %>%
  setFlag3(!is.na(qty),    type = 'method', flag = 'h', variable = 'quantity')


##' 1. Remove in ES those reporters with area codes that are not included in
##' MDB commodity mapping area list.

##+ es-treat-unmapped
esdata_not_area_in_fcl_mapping <- esdata %>%
  filter_(~!(reporter %in% unique(hsfclmap$area)))

rprt_writetable(esdata_not_area_in_fcl_mapping)

esdata <- filter_(esdata, ~reporter %in% unique(hsfclmap$area))

flog.info("Records after removing areas not in HS->FCL map: %s", nrow(esdata))

# ES trade data mapping to FCL ####
message(sprintf("[%s] Convert Eurostat HS to FCL", PID))

##' 1. Map HS codes to FCL.

##'     1. Extract HS6-FCL mapping table.

esdatahs6links <- mapHS6toFCL(esdata, hs6fclmap)

##'     1. Extract specific HS-FCL mapping table.

esdatalinks <- mapHS2FCL(tradedata   = esdata,
                         maptable    = hsfclmap3,
                         hs6maptable = hs6fclmap,
                         year        = year,
                         parallel    = multicore)

##'     1. Use HS6-FCL or HS-FCL mapping table.

esdata <- add_fcls_from_links(esdata,
                              hs6links = esdatahs6links,
                              links    = esdatalinks)

##'     1. Use HS6 standard for unmapped codes.

esdata <- esdata %>%
  left_join(
    hs6standard %>% select(-hs2012_code),
    by = 'hs6'
  ) %>%
  mutate(fcl = ifelse(is.na(fcl) & !is.na(faostat_code), faostat_code, fcl)) %>%
  select(-faostat_code)

flog.info("Records after HS-FCL mapping: %s", nrow(esdata))

rprt(esdata, "hs2fcl_fulldata", tradedataname = "esdata")

flog.trace("ES: dropping unmapped records", name = "dev")

##' 1. Remove unmapped FCL codes (i.e., transactions with no HS to FCL ' link).

esdata <- filter_(esdata, ~!(is.na(fcl)))

flog.info("ES records after removing non-mapped HS codes: %s", nrow(esdata))

##' 1. Add FCL units.

esdata <- addFCLunits(tradedata = esdata, fclunits = fclunits)

##' 1. Specific conversions: some FCL codes are reported in Eurostat
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
  left_join(es_spec_conv, by = 'fcl') %>%
  mutate_(qty = ~ifelse(is.na(conv), qty, qty*conv)) %>%
  setFlag3(!is.na(conv), type = 'method', flag = 'i', variable = 'quantity') %>%
  select_(~-conv)

##' # Specific operations on Tariff line data

##' 1. Do mathematical conversions on specific `qunit`s (6, 9, and 11 become 5).

# Convert qunit 6, 9, and 11 to 5 (mathematical conversion)
tldata <- as.data.table(tldata)
tldata[qunit ==  6, c('qty', 'qunit') := list(   qty*2, 5)]
tldata[qunit ==  9, c('qty', 'qunit') := list(qty*1000, 5)]
tldata[qunit == 11, c('qty', 'qunit') := list(  qty*12, 5)]
tldata <- tbl_df(tldata)

# tl-aggregate-multiple-rows ####

##' 1. Identical combinations of `reporter` / `partner` / `commodity` /
##' `flow` / `year` / `qunit` are pre-aggregated.

flog.trace("TL: aggreation of similar flows", name = "dev")

tldata <- preAggregateMultipleTLRows(tldata)

##' 1. Add variables that will contain flags. (Note: flags are set in various
##' steps in the code. Please, refer to the "Flag Management in the Trade module"
##' document.)

flog.trace("TL: add flag variables")
tldata <- generateFlagVars(tldata)

tldata <- tldata %>%
  setFlag3(nrows > 1, type = 'method', flag = 's', variable = 'all')


##' 1. Generate Observation Status "X" flag and Metdoh "h" flag.

tldata <- tldata %>%
  setFlag3(!is.na(value),  type = 'status', flag = 'X', variable = 'value') %>%
  setFlag3(!is.na(weight), type = 'status', flag = 'X', variable = 'weight') %>%
  setFlag3(!is.na(qty),    type = 'status', flag = 'X', variable = 'quantity') %>%
  setFlag3(!is.na(value),  type = 'method', flag = 'h', variable = 'value') %>%
  setFlag3(!is.na(weight), type = 'method', flag = 'h', variable = 'weight') %>%
  setFlag3(!is.na(qty),    type = 'method', flag = 'h', variable = 'quantity')


##+ drop_reps_not_in_mdb ####

##' 1. Area codes not mapping to any FAO country in the HS to FCL mapping
##' code are removed.

# We drop reporters that are absent in MDB hsfcl map
# because in any case we can proceed their data

tldata_not_area_in_fcl_mapping <- tldata %>%
  filter_(~!(reporter %in% unique(hsfclmap$area)))

rprt_writetable(tldata_not_area_in_fcl_mapping)

flog.trace("TL: dropping reporters not found in the mapping table", name = "dev")
tldata <- filter_(tldata, ~reporter %in% unique(hsfclmap$area))

##+ reexptoexp ####
##' 1. Re-imports become imports and re-exports become exports.
flog.trace("TL: recoding reimport/reexport", name = "dev")

# { "id": "1", "text": "Import" },
# { "id": "2", "text": "Export" },
# { "id": "4", "text": "re-Import" },
# { "id": "3", "text": "re-Export" }

tldata <- mutate_(tldata, flow = ~recode(flow, '4' = 1L, '3' = 2L))

##' 1. Map HS codes to FCL.
##+ tl_hs2fcl ####

##'     1. Extract HS6-FCL mapping table.

tldatahs6links <- mapHS6toFCL(tldata, hs6fclmap)

##'     1. Extract specific HS-FCL mapping table.

tldatalinks <- mapHS2FCL(tradedata   = tldata,
                         maptable    = hsfclmap3,
                         hs6maptable = hs6fclmap,
                         year        = year,
                         parallel    = multicore)

##'     1. Use HS6-FCL or HS-FCL mapping table.

tldata <- add_fcls_from_links(tldata,
                              hs6links = tldatahs6links,
                              links    = tldatalinks)

##'     1. Use HS6 starndard for unmapped codes.

tldata <- tldata %>%
  left_join(
    hs6standard %>% select(-hs2012_code),
    by = 'hs6'
  ) %>%
  mutate(fcl = ifelse(is.na(fcl) & !is.na(faostat_code), faostat_code, fcl)) %>%
  select(-faostat_code)

flog.info("Records after HS-FCL mapping: %s", nrow(tldata))

rprt(tldata, "hs2fcl_fulldata", tradedataname = "tldata")

flog.trace("TL: dropping unmapped records", name = "dev")

##' 1. Remove unmapped FCL codes (i.e., transactions with no HS to FCL ' link).

tldata <- filter_(tldata, ~!is.na(fcl))

flog.info("TL records after removing non-mapped HS codes: %s", nrow(tldata))

if (stop_after_mapping) stop("Stop after HS->FCL mapping")

############# Units of measurment in TL ####

##' 1. Add FCL units.

flog.trace("TL: add FCL units", name = "dev")

tldata <- addFCLunits(tldata, fclunits = fclunits)

tldata <- tldata %>%
  mutate_(qunit = ~as.integer(qunit)) %>%
  left_join(comtradeunits %>% select_(~qunit, ~wco),
            by = "qunit")

## Dataset with all matches between Comtrade and FAO units
ctfclunitsconv <- tldata %>%
  select_(~qunit, ~wco, ~fclunit) %>%
  distinct() %>%
  arrange_(~qunit) %>%
  as.data.table()

################ Conv. factor (TL) ################
flog.trace("TL: conversion factors", name = "dev")

##### Table for conv. factor

##' 1. General conversions: some FCL codes are reported in Tariffline with
##' different units than those reported in FAOSTAT, thus a conversion is done.

ctfclunitsconv$conv <- 0
# Missing quantity
ctfclunitsconv[qunit == 1,                               conv :=   NA]
# Missing quantity
ctfclunitsconv[fclunit == "$ value only",                conv :=   NA]
ctfclunitsconv[fclunit == "mt"         & wco == "l",     conv := .001]
ctfclunitsconv[fclunit == "heads"      & wco == "u",     conv :=    1]
ctfclunitsconv[fclunit == "1000 heads" & wco == "u",     conv := .001]
ctfclunitsconv[fclunit == "number"     & wco == "u",     conv :=    1]
ctfclunitsconv[fclunit == "mt"         & wco == "kg",    conv := .001]
ctfclunitsconv[fclunit == "mt"         & wco == "m³",    conv :=    1]
ctfclunitsconv[fclunit == "mt"         & wco == "carat", conv := 5e-6]


##### Add conv factor to the dataset

tldata <- left_join(tldata, ctfclunitsconv, by = c("qunit", "wco", "fclunit"))

##' 1. Specific conversions: some FCL codes are reported in Tariff line
##' with different supplementary units than those reported in FAOSTAT,
##' thus a conversion is done.

#### Commodity specific conversion

fcl_spec_mt_conv <- tldata %>%
  filter_(~fclunit == "mt" & is.na(weight) & conv == 0) %>%
  select_(~fcl, ~wco) %>%
  distinct

if (NROW(fcl_spec_mt_conv) > 0) {

  conversion_factors_fcl <- tldata %>%
    filter(!is.na(weight) & !is.na(qty)) %>%
    mutate(qw = (weight/qty)/1000) %>%
    group_by(fcl, wco) %>%
    summarise(convspec = median(qw, na.rm = TRUE)) %>%
    ungroup()

  fcl_spec_mt_conv <- fcl_spec_mt_conv %>%
    left_join(conversion_factors_fcl, by = c("fcl", "wco"))

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

##' 1. If the `weight` variable is available and the final unit
##' of measurement is tonnes then `weight` is used as `quantity`.

cond <- tldata$fclunit == 'mt' & !is.na(tldata$weight) & tldata$weight > 0

tldata$qtyfcl <- ifelse(cond, tldata$weight*0.001, tldata$qtyfcl)

# XXX
# Flag on weight as qty (which underwent a change) will populate weight
tldata <- tldata %>%
  setFlag3(cond, type = 'method', flag = 'i', variable = 'weight')

######### Value from USD to thousands of USD

##' 1. Convert data in thousands of dollars.

if (dollars) {
  esdata <- esdata %>%
    mutate(value = value * 1000) %>%
    setFlag3(value > 0, type = 'method', flag = 'i', variable = 'value')
} else { ## This means it is in k$
  tldata <- tldata %>%
    mutate(value = value / 1000) %>%
    setFlag3(value > 0, type = 'method', flag = 'i', variable = 'value')
}

##' 1. Aggregate data to FCL level.

##+ tl_aggregate

# Replace weight (first quantity column) by newly produced qtyfcl column
# XXX "notes" are applied to weight that is transformed below from qtyfcl
flog.trace("TL: aggregate to FCL", name = "dev")
tldata <- tldata %>%
  select(-weight, -qty) %>%
  rename(weight = qtyfcl) # XXX weight should probably be renamed qty here

tldata_mid = tldata

  ##' 1. Application of "adjustment notes" to both ES and TL data.

# TODO Check quantity/weight
# The notes should save the results in weight

# TODO (Christian) Check this (some ES partners are not TL partners):
# unique(esdata$partner)[!(unique(esdata$partner) %in% unique(tldata$partner))]

# We need to set the flags one by one as adjustments not necessarily
# (probably never?) adjust all the three variables at the same time
if (use_adjustments == TRUE) {
  flog.trace("Apply adjustments", name = "dev")
  esdata <- useAdjustments(tradedata = esdata, year = year, PID = PID,
                           adjustments = adjustments, parallel = multicore) %>%
    setFlag3(adj_value  == TRUE, type = 'method', flag = 'i', variable = 'value') %>%
    setFlag3(adj_weight == TRUE, type = 'method', flag = 'i', variable = 'weight') %>%
    setFlag3(adj_qty    == TRUE, type = 'method', flag = 'i', variable = 'quantity')

  tldata <- useAdjustments(tradedata = tldata, year = year,
                           adjustments = adjustments, parallel = multicore) %>%
    setFlag3(adj_value  == TRUE, type = 'method', flag = 'i', variable = 'value') %>%
    setFlag3(adj_weight == TRUE, type = 'method', flag = 'i', variable = 'weight') %>%
    setFlag3(adj_qty    == TRUE, type = 'method', flag = 'i', variable = 'quantity')
}

##+ es_convcur

##' 1. Convert currency of monetary values from EUR to USD using the
##' `EURconversionUSD` table.

esdata$value <- esdata$value * as.numeric(EURconversionUSD %>%
                                          filter(eusd_year == year) %>%
                                          select(eusd_exchangerate))

esdata <- esdata %>%
    setFlag3(value > 0, type = 'method', flag = 'i', variable = 'value')

  ###' 1. Assign 'weight' flags to 'qty' flags in TL XXX.
  #
  # NO: this isn't needed as below qty = weight and it has already its own flag
  #
  #tldata <- tldata %>%
  #  mutate_each_(funs(swapFlags(., swap='\\1\\2\\2'), !is.na(weight)),
  #               ~starts_with('flag_'))

# Assign `qty` flags to `weight` flags in ES but
# only when `fclunit` is different from "mt".

esdata <- esdata %>%
  mutate_each_(funs(swapFlags(., swap='\\1\\3\\3', fclunit != "mt")),
               ~starts_with('flag_'))

##' # Combine Trade Data Sources

##' 1. Combine Tariff line and Eurostat data sources in a single data set:
##'     - TL: assign `weight` to `qty`.
##'     - ES: assign `weight` to `qty` if `fclunit` is "mt", else keep `qty`.

##+ combine_es_tl
flog.trace("Combine TL and ES data sets", name = "dev")
tradedata <- bind_rows(
  tldata %>%
    select(year, reporter, partner, flow,
            fcl, fclunit, hs,
            qty = weight, value,
            starts_with('flag_')),
  esdata %>%
    mutate(uniqqty = ifelse(fclunit == "mt", weight, qty)) %>%
    select(year, reporter, partner, flow,
            fcl, fclunit, hs,
            qty = uniqqty, value,
            starts_with('flag_'))
)

# XXX this is fine, but probably the name of the function should be changed
tradedata <- tradedata %>%
  mutate_each_(funs(swapFlags(., swap='\\1\\2')), ~starts_with('flag_'))

### Check for double counting of HS codes
#hs_many_lengths = getHsManyLengths(tradedata)
#rprt_writetable(hs_many_lengths, subdir = 'details')

##' # Imputation
flog.trace("Outlier detection and imputation", name = "dev")
##+ calculate_median_uv

tradedata <- tradedata %>%
  mutate_(no_quant = ~near(qty, 0) | is.na(qty),
          no_value = ~near(value, 0) | is.na(value))

##' 1. Unit values are calculated for each observation at the HS level as ratio
##' of monetary value over quantitya: $uv = value / qty$.

tradedata <- mutate_(tradedata,
                     uv = ~ifelse(no_quant | no_value, NA, value / qty))

## Round UV in order to avoid floating point number problems (see issue #54)
tradedata$uv <- round(tradedata$uv, 10)

##+ boxplot_uv

if (detect_outliers) {
  tradedata <- detectOutliers(tradedata = tradedata,
                              method = "boxplot",
                              parameters = list(out_coef = out_coef))
} else {
  tradedata$outlier <- FALSE
}

##+ impute_qty_uv

##' 1. Imputation of missing quantities by applying the method presented
##' in the *Missing Quantities Imputation* subsection of the *faoswsTrade:
##' `complete_tf_cpc` and `total_trade_CPC` modules* document
##' (*Standardization, editing and outlier detection* section). The
##' `flagTrade` variable is given a value of 1 if an imputation was performed.

## These flags are also assigned to monetary values. This may need to be
## revised (monetary values are not supposed to be modified).

tradedata <- computeMedianUnitValue(tradedata = tradedata)

tradedata <- doImputation(tradedata = tradedata)

flog.trace("Flag stuff", name = "dev")
# XXX using flagTrade for the moment, but should go away
tradedata <- tradedata %>%
    setFlag2(flagTrade > 0, type = 'status', flag = 'I', var = 'quantity') %>%
    setFlag2(flagTrade > 0, type = 'method', flag = 'e', var = 'quantity')

##' Separate flags.

###### TODO (Christian) Rethink/refactor
# separate flag_method and flag_status into 2 variables each one: _v and _q
flag_vars <- colnames(tradedata)[grep('flag_', colnames(tradedata))]
for (var in flag_vars) {
  tradedata <- separate_(tradedata, var, 1:2,
                         into = c('x', paste0(var, '_', c('v', 'q'))),
                         convert = TRUE) %>%
               select(-x)
}

tradedata_flags <- tradedata %>%
  group_by_(~year, ~reporter, ~partner, ~flow, ~fcl) %>%
  summarise_each_(funs(sum(.)), vars = ~starts_with('flag_')) %>%
  ungroup() %>%
  mutate_each_(funs(as.integer(. > 0)), vars = ~starts_with('flag_'))

##' 1. Aggregate values, quantities, and flags by FCL codes.

# Aggregation by fcl
flog.trace("Aggregation by FCL", name = "dev")
tradedata <- tradedata %>%
  mutate_(nfcl = 1) %>%
  group_by_(~year, ~reporter, ~partner, ~flow, ~fcl, ~fclunit) %>%
  summarise_each_(funs(sum(., na.rm = TRUE)),
                  vars = c("qty", "value","flagTrade", "nfcl")) %>%
  ungroup()

flog.trace("Flags again", name = "dev")
tradedata <- left_join(tradedata,
                       tradedata_flags,
                       by = c('year', 'reporter', 'partner', 'flow', 'fcl'))

###### TODO (Christian) Rethink/refactor
# unite _v and _q into one variable
flag_vars <- sort(unique(sub('_[vq]$', '', colnames(tradedata)[grep('flag_', colnames(tradedata))])))
for (var in flag_vars) {
  var_v <- paste0(var, '_v')
  var_q <- paste0(var, '_q')

  tradedata[[var]] <- 100 + (tradedata[[var_v]]>0)*10 + (tradedata[[var_q]]>0)*1
}
tradedata <- tradedata[-grep('^flag_.*[vq]$', colnames(tradedata))]

tradedata <- tradedata %>%
  setFlag2(nfcl > 1,  type = 'method', flag = 's', variable = 'all')

##' 1. Map FCL codes to CPC.

# Adding CPC2 extended code
flog.trace("Add CPC item codes", name = "dev")
tradedata <- tradedata %>%
  mutate_(cpc = ~fcl2cpc(sprintf("%04d", fcl), version = "2.1"))

# Not resolve mapping fcl2cpc
no_mapping_fcl2cpc = tradedata %>%
  select_(~fcl, ~cpc) %>%
  filter_(~is.na(cpc)) %>%
  distinct_(~fcl) %>%
  select_(~fcl) %>%
  unlist()

##' 1. Map FAO area codes to M49. Countries with FAOSTAT code 252
##' ("Unspecified") are converted to M49 code 896 ("Other nei").

# Converting back to M49 for the system
flog.trace("Convert FAO area codes to M49", name = "dev")
tradedata <- tradedata %>%
  mutate_(reporterM49 = ~fs2m49(as.character(reporter)),
          partnerM49  = ~fs2m49(as.character(partner))) %>%
  # XXX issue 34
  mutate(partnerM49 = ifelse(partner == 252, '896', partnerM49))

# Report of countries mapping to NA in M49
countries_not_mapping_M49 <- bind_rows(
  tradedata %>% select_(fc = ~reporter, m49 = ~reporterM49),
  tradedata %>% select_(fc = ~partner, m49 = ~partnerM49)) %>%
  distinct_() %>%
  filter_(~is.na(m49)) %>%
  select_(~fc) %>%
  unlist()

##+ mirror_estimation

##' # Mirror Trade Estimation

##' 1. Create a table with the list of reporters and partners
##' combined as areas and count the number of flows that the
##' areas declare as reporting countries. The partners that
##' never show up as reporters or the reporters that do not
##' report a flow will have a number of flows equal to zero
##' and will be mirrored.
flog.trace("Mirroring", name = "dev")

to_mirror <- flowsToMirror(tradedata) %>%
  filter(area != 252)

##' 1. Swap the reporter and partner dimensions: the value previously appearing
##' as reporter country code becomes the partner country code (and vice versa).

##' 1. Invert the flow direction: an import becomes an export (and vice versa).

##' 1. Calculate monetary mirror value by adding (removing) a 12% mark-up on
##' imports (exports) to account for the difference between CIF and FOB prices.

## Mirroring for non reporting countries
tradedata <- mirrorNonReporters(tradedata, mirror = to_mirror)

# Add an auxiliary variable "mirrored" that will be removed later
tradedata <-
  left_join(
    tradedata,
    to_mirror %>% mutate(mirrored = 1L),
    by = c('reporter' = 'area', 'flow')
  )

flog.trace("Flags to mirrored flows", name = "dev")

tradedata <- tradedata %>%
  setFlag2(!is.na(mirrored), type = 'status', flag = 'T', var = 'all') %>%
  setFlag2(!is.na(mirrored), type = 'method', flag = 'i', var = 'value') %>%
  setFlag2(!is.na(mirrored), type = 'method', flag = 'c', var = 'quantity') %>%
  select(-mirrored)

##' ## Flag aggregation

##' Flags are aggregated as mentioined in the *Flags* section in
##' the main documentation or, more in depth, in the "Flag Management
##' in the Trade module" document.

################################################
# TODO Rethink/refactor: clean flags for fclunit != "$ value only"
################################################

##+ completed_trade_flow

###### TODO (Christian) Rethink/refactor
# separate flag_method and flag_status into 2 variables each one: _v and _q
flag_vars <- colnames(tradedata)[grep('flag_', colnames(tradedata))]
for (var in flag_vars) {
  tradedata <- separate_(tradedata, var, 1:2,
                         into = c('x', paste0(var, '_', c('v', 'q'))),
                         convert = TRUE) %>%
               select(-x)
}

##' # Output for SWS

##' 1. Filter observations with FCL code `1181` (bees).

##' 1. Filter observations with missing CPC codes.

##' 1. Rename dimensions to comply with SWS standard,
##' e.g., `geographicAreaM49Reporter`.

##' 1. Calculate unit value (US$ per quantity unit) at CPC
##' level if the quantity is larger than zero.

# Modified in order to have X in the table
flagWeightTable_status <- frame_data(
  ~flagObservationStatus, ~flagObservationWeights,
  'X',                   1.00,
  '',                    0.99,
  'T',                   0.80,
  'E',                   0.75,
  'I',                   0.50,
  'M',                   0.00
)

# There is no native "method" table
flagWeightTable_method <- frame_data(
  ~flagObservationStatus, ~flagObservationWeights,
  'h',                   1.00,
  'i',                   0.80,
  'e',                   0.60,
  'c',                   0.40,
  's',                   0.20
)

# XXX This piece of code is really slow. There should be a better way.
flog.trace("Cycle on status and method flags", name = "dev")
for (i in c('status', 'method')) {
  for (j in c('v', 'q')) {

    dummies <- tradedata %>%
      select(starts_with(paste0('flag_', i))) %>%
      select(ends_with(j))

    flags <- sub('.*_(.)_.$', '\\1', colnames(dummies))

    if (i == 'status') {
      flagWeightTable <- flagWeightTable_status
    } else {
      flagWeightTable <- flagWeightTable_method
    }

    var <- paste0('flag', toupper(i), '_', j)

    tradedata[[var]] <- apply(dummies, 1, function(x)
                              ifelse(sum(x)==0, NA,
                                     aggregateObservationFlag(flags[x==1])))
  }
}

flog.trace("Complete trade flow CPC", name = "dev")
complete_trade_flow_cpc <- tradedata %>%
  filter_(~fcl != 1181) %>% ## Subsetting out bees
  select_(~-fcl) %>%
  filter_(~!(is.na(cpc))) %>%
  transmute_(geographicAreaM49Reporter = ~reporterM49,
             geographicAreaM49Partner  = ~partnerM49,
             flow                      = ~flow,
             timePointYears            = ~year,
             flagObservationStatus_v   = ~flagSTATUS_v,
             flagObservationStatus_q   = ~flagSTATUS_q,
             flagMethod_v              = ~flagMETHOD_v,
             flagMethod_q              = ~flagMETHOD_q,
             measuredItemCPC           = ~cpc,
             qty                       = ~qty,
             unit                      = ~fclunit,
             value                     = ~value) %>%
  ## unit of monetary values is "1000 $"
  mutate(uv = ifelse(qty > 0, value * 1000 / qty, NA))


##' 1. Use corrections from validation
if (CheckDebug()) {
  corrections_table_all <- readRDS('//hqlprsws1.hq.un.fao.org/sws_r_share/trade/validation_tool_files/corrections_table.rds')
} else {
  corrections_table_all <- readRDS('/work/SWS_R_Share/trade/validation_tool_files/corrections_table.rds')
}

corrections_table <- corrections_table_all %>%
  rename(correction_year = year) %>%
  filter(correction_year == year)

corrections_exist <- nrow(corrections_table) > 0

if (corrections_exist) {
  corrections_table <- corrections_table %>%
    filter(correction_level == 'CPC') %>%
    select(-correction_year, -correction_level, -correction_hs) %>%
    # Some of these cases were found, but are probably mistakes: should inform
    filter(!is.na(correction_input) | !near(correction_input, 0)) %>%
    # XXX actually, flow should be integer in complete_trade_flow_cpc
    mutate(flow = as.numeric(flow)) %>%
    # XXX Remove duplicate corrections
    group_by(reporter, partner, item, flow, data_type) %>%
    arrange(desc(date_correction)) %>%
    mutate(i = 1:n()) %>%
    filter(i == 1L) %>%
    ungroup() %>%
    select(-i)

  corrections_metadata <- apply(select(corrections_table, name_analyst, data_original, correction_type:date_validation),
                                1, function(x) paste(names(x), ifelse(x == '', NA, x), collapse = '; ', sep = ': '))

  corrections_table <- corrections_table %>%
    select(-(correction_note:date_validation)) %>%
    mutate(correction_metadata = gsub('  *', ' ', corrections_metadata))

  complete_corrected <- useValidationCorrections(complete_trade_flow_cpc, corrections_table)

  complete_trade_flow_cpc_mirror <- complete_trade_flow_cpc %>%
    mutate(is_mirror = (flagObservationStatus_v %in% 'T' | flagObservationStatus_q %in% 'T')) %>%
    filter(is_mirror) %>%
    select(-is_mirror)

  complete_with_corrections_mirror <- complete_corrected$corrected %>%
    select(
      geographicAreaM49Reporter = geographicAreaM49Partner,
      geographicAreaM49Partner  = geographicAreaM49Reporter,
      flow,
      measuredItemCPC
    ) %>%
    mutate(flow = recode(flow, '2' = 1, '1' = 2), to_correct = TRUE)

  complete_mirror_to_correct <-
    left_join(
      complete_trade_flow_cpc_mirror,
      complete_with_corrections_mirror,
      by = c(
        'geographicAreaM49Reporter',
        'geographicAreaM49Partner',
        'flow',
        'measuredItemCPC'
      )
    ) %>%
    filter(to_correct) %>%
    select(-to_correct)

  corrections_table_mirror <- corrections_table %>%
    rename(reporter = partner, partner = reporter) %>%
    mutate(flow = recode(flow, '2' = 1, '1' = 2)) %>%
    mutate(correction_input = ifelse(data_type == 'value', ifelse(flow == 1, correction_input * 1.12, correction_input / 1.12), correction_input))

  complete_mirror_corrected <- useValidationCorrections(complete_mirror_to_correct, corrections_table_mirror)

  complete_all_corrected <- bind_rows(complete_corrected$corrected, complete_mirror_corrected$corrected)

  complete_uncorrected <- anti_join(
                            complete_trade_flow_cpc,
                            complete_all_corrected,
                            by = c('geographicAreaM49Reporter', 'geographicAreaM49Partner', 'flow', 'measuredItemCPC')
                          )

  complete_trade_flow_cpc <- bind_rows(complete_uncorrected, complete_all_corrected)

  if (nrow(complete_corrected$to_drop) > 0) {
    # XXX this should go in a CSV file
    warning('Some corrections were not applied:')
    complete_corrected$to_drop %>%
      mutate(year = year) %>%
      select(year, everything()) %>%
      as.data.frame()
  }

} else {
  complete_trade_flow_cpc <- complete_trade_flow_cpc %>%
    mutate(
      correction_metadata_qty   = NA_character_,
      correction_metadata_value = NA_character_,
      correction_metadata_uv    = NA_character_
    )
}

##' 1. Transform dataset separating monetary values, quantities and unit values
##' in different rows.

##' 1. Convert monetary values, quantities and unit values to corresponding SWS
##' element codes. For example, a quantity import measured in metric tons is
##' assigned `5610`.

##+ convert_element

quantityElements <- c("5608", "5609", "5610", "5908", "5909", "5910")
uvElements       <- c("5638", "5639", "5630", "5938", "5939", "5930")

complete_trade_flow_cpc <- complete_trade_flow_cpc %>%
  tidyr::gather(measuredElementTrade, Value, -geographicAreaM49Reporter,
                -geographicAreaM49Partner, -measuredItemCPC, -timePointYears,
                -flagObservationStatus_v, -flagObservationStatus_q,
                -flagMethod_v, -flagMethod_q, -unit, -flow,
                -correction_metadata_qty, -correction_metadata_value) %>%
  rowwise() %>%
  mutate_(measuredElementTrade =
            ~convertMeasuredElementTrade(measuredElementTrade,
                                         unit,
                                         flow)) %>%
  ungroup() %>%
  filter_(~measuredElementTrade != "999") %>%
  mutate(
    correction_metadata_uv = ifelse(
                               !is.na(correction_metadata_qty),
                               ifelse(
                                 !is.na(correction_metadata_value),
                                 paste('QTY:', correction_metadata_qty, '| VALUE:', correction_metadata_value),
                                 correction_metadata_qty
                               ),
                               correction_metadata_value
                             ),
    correction_metadata   = ifelse(
                              measuredElementTrade %in% quantityElements,
                              correction_metadata_qty,
                              ifelse(
                                measuredElementTrade %in% uvElements,
                                correction_metadata_uv,
                                correction_metadata_value
                              )
                            )
  ) %>%
  select_(~-flow,~-unit, ~-correction_metadata_qty, ~-correction_metadata_value, ~-correction_metadata_uv)

if (corrections_exist) {
##' 1. Generate metadata for corrections.

  metad <- complete_trade_flow_cpc %>%
    filter(!is.na(correction_metadata)) %>%
    select(
      geographicAreaM49Reporter,
      geographicAreaM49Partner,
      measuredElementTrade,
      measuredItemCPC,
      timePointYears,
      correction_metadata
    ) %>%
    mutate(
      Metadata          = "GENERAL",
      Metadata_Element  = "COMMENT",
      Metadata_Language = "en",
      Metadata_Value    = correction_metadata
    ) #%>%
    ### NOTE: metadata can be splitted as shown below, though there is
    ### still some work to do on how to store metadata of unit values
    #separate(
    #  correction_metadata, # Or Metadata_Value, if computed above
    #  into = c(
    #    'name_analyst',
    #    'data_original',
    #    'correction_type',
    #    'correction_note',
    #    'note_analyst',
    #    'note_supervisor',
    #    'name_supervisor',
    #    'date_correction',
    #    'date_validation'
    #  ),
    #  sep = ' *; *'
    #) %>%
    #gather(
    #  key,
    #  value,
    #  name_analyst,
    #  data_original,
    #  correction_type,
    #  correction_note,
    #  note_analyst,
    #  note_supervisor,
    #  name_supervisor,
    #  date_correction,
    #  date_validation
    #) %>%
    #select(-key) %>%
    #rename(Metadata_Value = value)

  # Required to be a data.table
  metad <- select(metad, -correction_metadata) %>% as.data.table()
}

complete_trade_flow_cpc <- complete_trade_flow_cpc %>%
  select(-correction_metadata) %>%
  mutate(
    flagObservationStatus = ifelse(measuredElementTrade %in% quantityElements,
                                   flagObservationStatus_q,
                                   flagObservationStatus_v),
    flagMethod            = ifelse(measuredElementTrade %in% quantityElements,
                                   flagMethod_q,
                                   flagMethod_v)
  ) %>%
  # The Status flag will be equal to the weakest flag between
  # the numerator and the denominator, in this case the denominator.
  mutate(
    flagObservationStatus = ifelse(measuredElementTrade %in% uvElements,
                                   flagObservationStatus_q,
                                   flagObservationStatus),
    flagMethod = ifelse(measuredElementTrade %in% uvElements,
                        'i',
                        flagMethod)
  ) %>%
  select(-flagObservationStatus_v, -flagObservationStatus_q,
         -flagMethod_v, -flagMethod_q)

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

# XXX Temporary workaround: some NAs are given flags and given
# that NAs cannot have flags the system refuses to save them.
# These NAs are unit values computed on a zero quantity. Setting
# Value to zero.
complete_trade_flow_cpc[is.na(Value), Value := 0]

# "official" status flag should be <BLANK> instead of X (this was a choice
# made after X was chosen as official flag). Thus, change X to <BLANK>.
complete_trade_flow_cpc[flagObservationStatus == 'X', flagObservationStatus := '']

##' # Save the `completed_tf_cpc_m49` dataset to the `trade` domain

flog.trace("[%s] Writing data to session/database", PID, name = "dev")
if (corrections_exist) {
  stats <- SaveData("trade",
                    "completed_tf_cpc_m49",
                    complete_trade_flow_cpc,
                    metadata    = metad,
                    waitTimeout = 10800)
} else {
  stats <- SaveData("trade",
                    "completed_tf_cpc_m49",
                    complete_trade_flow_cpc,
                    waitTimeout = 10800)
}

## remove value only

flog.trace("[%s] Session/database write completed!", PID, name = "dev")

flog.info(
  "Module completed in %1.2f minutes.
  Values inserted: %s
  appended: %s
  ignored: %s
  discarded: %s",
    difftime(Sys.time(), startTime, units = "min"),
    stats[["inserted"]],
    stats[["appended"]],
    stats[["ignored"]],
    stats[["discarded"]], name = "dev"
  )

# Restore changed options
options(old_options)

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

