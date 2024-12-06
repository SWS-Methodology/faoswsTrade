

################################################################################
#
# Type: plugin.
#
# Name: ti_IDR_SSR.
#
# Description: R script to calculate the Import Dependency Ratio (IDR) and the 
#              Self-Sufficiency Ratio (SSR) starting from elements 5510 
#              (Production), 5610 (Import Quantity), and 5910 (Export quantity).
#              The following is a brief description of the indicators.
#              IDR: measures a country's import dependency on a specific 
#                   commodity. A higher ratio indicates that the country is more
#                   import dependent on that commodity, while negative values 
#                   denote that the country is a net exporter.  
#              SSR: shows the magnitude of production in relation to domestic
#                   supply. A higher ratio indicates that the country is more
#                   self-sufficient on that commodity, while a ratio less than 1
#                   implies that food production is insufficient to meet the 
#                   population demand.
#
# Author's email: riccardo.giubilei@fao.org
#
################################################################################



# Plugin initialization ---------------------------------------------------

# Plugin name
plugin_name <- 'ti_IDR_SSR'

# Print starting message
message(paste("The", plugin_name, "plugin is running."))



# Packages ----------------------------------------------------------------

# Print packages-loading message
message(paste("The", plugin_name, "plugin is loading packages."))

# Load packages
suppressMessages({
  library(faosws)
  library(faoswsUtil)
  library(faoswsFlag)
  library(data.table)
}) 


# Environment -------------------------------------------------------------

if (faosws::CheckDebug()) {
  
  # Connect to .yml file
  library(faoswsModules)
  SETT <- faoswsModules::ReadSettings("sws_trade_indicators.yml")
  
  # Set up offline environment
  server <- SETT[["server"]]
  token <- SETT[["token"]]
  faosws::GetTestEnvironment(baseUrl = server,
                             token = token)
  
  # Load helper functions
  source('plugin_helper_functions.R')
  
}


# Import datatables -------------------------------------------------------

# Print datatable-importing message
message(paste("The", plugin_name, "plugin is importing datatables."))


### Key commodities datatable ###

# Get item codes from key commodities datatable
cpc_codes <- faosws::ReadDatatable(table = "key_commodities_codes")$commodity_code


### Reporter countries ###

# Import reporter countries datatable
reporters_by_year <- faosws::ReadDatatable(table = "reporters_by_year_new_version")

# Get GeographicArea codes from reporter countries
m49_codes <- unique(reporters_by_year$m49)

# Get years from reporter countries
reporters_cols <- colnames(reporters_by_year)
years_cols <- grep('year_', reporters_cols, value = TRUE)
years_reporters <- sub('year_', '', years_cols)

# Exclude years for which there are no observations
na_number <- colSums(is.na(reporters_by_year))
empty_cols <- reporters_cols[na_number == nrow(reporters_by_year)]
NA_years_reporters <- sub('year_', '', empty_cols)
years_reporters <- setdiff(years_reporters,
                           NA_years_reporters)
# NB: this is not strictly necessary as countryReports() already excludes these 
#     years, but it allows to download the minimal set of data that is required.

# NB: years can be possibly further refined using input parameters.



# Input parameters --------------------------------------------------------

# Print input prameters message
message(paste("The", plugin_name, "plugin is reading the input parameters."))

### Input domains ###

# Retrieve parameters' values
agriculture_dataset <- swsContext.computationParams$agriculture_source
trade_dataset <- swsContext.computationParams$trade_source


### Years ###

# Retrieve parameters' values
starting_year <- swsContext.computationParams$starting_year
ending_year <- swsContext.computationParams$ending_year

# If NULL, replace them with years from reporter countries datatable
if (is.null(starting_year)) starting_year <- min(years_reporters)
if (is.null(ending_year)) ending_year <- max(years_reporters)

# Check that the starting year does not come after the ending year
stopifnot("The starting year cannot come after the ending year." = 
            starting_year <= ending_year)

# Compute input year-range 
input_years <- starting_year:ending_year

# Final set of years as intersection between reporters and user interface input
selected_years <- intersect(years_reporters,
                            input_years)


### Query-only parameter ###

# Retrieve parameter's value
query_only <- swsContext.computationParams$query_only

# If query_only is 'yes', retrieve input keys from the query
if (query_only == 'yes') {
  
  # Input keys
  query_geo <- query_keys('geographicAreaM49')
  query_elem <- query_keys('measuredElementTrade')
  query_item <- query_keys('measuredItemCPC')
  query_years <- query_keys('timePointYears')
  # N.B.: query_elem is only used to choose which results to return; the others
  #       are used to get input data, and will override the three corresponding
  #       vectors of keys.
  
  # Replace m49_codes with the query_geo that are in m49_codes (reporter 
  # countries), if any; otherwise, throw warning and use the original m49_codes
  if (any(query_geo %in% m49_codes)) {
    m49_codes <- query_geo[query_geo %in% m49_codes]
  } else {
    warning('The queried key(s) for geographicAreaM49 is not present among the reporter countries. All the reporter countries will be considered instead.')
  }
  
  # Replace cpc_codes with the query_item that are in cpc_codes (key
  # commodities), if any; otherwise, throw warning and use the original cpc_codes
  if (any(query_item %in% cpc_codes)) {
    cpc_codes <- query_item[query_item %in% cpc_codes]
  } else {
    warning('The queried key(s) for measuredItemCPC is not present among the key commodities. All the key commodities will be considered instead.')
  }
  
  # Replace selected_years with the query_years that are in selected_years, if
  # any; otherwise, throw warning and use the original selected_years
  if (any(query_years %in% selected_years)) {
    selected_years <- query_years[query_years %in% selected_years]
  } else {
    warning('The queried key(s) for timePointsYears is not included in the years specified when running the plugin. The latter are used instead.')
  }
  
}



# Import data -------------------------------------------------------------

# Print input data-importing message
message(paste("The", plugin_name, "plugin is importing input data."))

# Agriculture dataset: Production [t] (5510)
data_agr <- sws2dataset(dataset = agriculture_dataset,
                        keys = list(geo = m49_codes,
                                    elem = '5510',   
                                    item = cpc_codes,
                                    years = selected_years))

# Trade dataset: Import Quantity (5610) and Export Quantity (5910)
data_trade <- sws2dataset(dataset = trade_dataset,
                          keys = list(geo = m49_codes,
                                      elem = c('5610', '5910'),   
                                      item = cpc_codes,
                                      years = selected_years),
                          elem_colnames = c('import', 'export'))



# IDR and SSR indicators --------------------------------------------------

# Print indicator-calculation message
message(paste("The", plugin_name, "plugin is computing the indicators."))

# Merge agricultural production and import/export
data_idrssr <- merge(x = data_agr,
                     y = data_trade,
                     all = T)

# Filter out years for which countries do not report
is.reporter <- countryReports(countries = data_idrssr$geographicAreaM49,
                              years = data_idrssr$timePointYears)
data_idrssr <- data_idrssr[is.reporter]

# Import Dependency Ratio
data_idrssr[, idr := (import - export) / (production + import - export)]

# Self-Sufficiency Ratio
data_idrssr[, ssr := production / (production + import - export)]

# Make all-zero cases return NA
data_idrssr[production == 0 & import == 0 & export == 0, c('idr', 'ssr')] <- NA_real_

# Make all cases where Y + M = X return NA
data_idrssr[production + import == export, c('idr', 'ssr')] <- NA_real_

# Check that IDR and SSR always sum to 1
stopifnot("IDR and SSR do not sum up to 1." =
            all(round(data_idrssr$idr + data_idrssr$ssr,
                      digits = 10) == 1,
                na.rm = TRUE))



# Save back the results and finalize --------------------------------------

# Print save message
message(paste("The", plugin_name, "plugin is saving the results back to the session."))

# If query_elem does not exist, initialize it
if (isFALSE(exists('query_elem'))) query_elem <- '0'

# Throw warning if none of the indicators is included in the queried elements
select_elem <- intersect(c('501', '502'),
                         query_elem)
nelem <- length(select_elem)
if (nelem == 0)
  warning('The indicators are not included in the queried key(s) for measuredElementTrade. Both of them will be returned.')

# Check which indicator(s) should be computed
compute_idr <- if ('501' %in% select_elem | nelem == 0) TRUE else FALSE
compute_ssr <- if ('502' %in% select_elem | nelem == 0) TRUE else FALSE

# IDR
if (isTRUE(compute_idr)) {
  
  # Save the results back to the session
  idr_save <- 
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators',
                     data = data_idrssr[, .(geographicAreaM49 = geographicAreaM49,
                                            measuredItemCPC = measuredItemCPC,
                                            timePointYears = timePointYears,
                                            Value = idr,
                                            measuredElementTrade = '501',
                                            flagObservationStatus = fifelse(is.na(get('idr')),
                                                                            NA_character_,
                                                                            'E'),
                                            flagMethod = fifelse(is.na(get('idr')),
                                                                 NA_character_,
                                                                 'i'))],
                     waitTimeout = 100000)
  
  # Log
  base_string <- paste("Log for the IDR indicator:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(idr_save[1:4], FUN = identity))))
  
}

# SSR
if (isTRUE(compute_ssr)) {
  
  # Save the results back to the session
  ssr_save <- 
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators',
                     data = data_idrssr[, .(geographicAreaM49 = geographicAreaM49,
                                            measuredItemCPC = measuredItemCPC,
                                            timePointYears = timePointYears,
                                            Value = ssr,
                                            measuredElementTrade = '502',
                                            flagObservationStatus = fifelse(is.na(get('ssr')),
                                                                            NA_character_,
                                                                            'E'),
                                            flagMethod = fifelse(is.na(get('ssr')),
                                                                 NA_character_,
                                                                 'i'))],
                     waitTimeout = 100000)
  
  # Log
  base_string <- paste("Log for the SSR indicator:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(ssr_save[1:4], FUN = identity))))
  
}

# Print information
#print(sessionInfo())
#print(version)

# Print ending message 
message(paste("Process completed successfully!"))



# Bin ---------------------------------------------------------------------

# Check that input years are in the correct format
#stopifnot("The starting year should be a number in the format yyyy." = 
#            nchar(starting_year) == 4L) 
#stopifnot("The ending year should be a number in the format yyyy." = 
#            nchar(ending_year) == 4L)
# No longer needed because now years must be between 1900 (minimum) and 2100 (maximum).

# Find input year-range and check that values are OK 
# (For example, if the values are not only numeric, the operator ":" does not work.)
#input_years <- tryCatch(suppressWarnings(starting_year:ending_year),
#                        error = function (e) return(NULL))
#stopifnot("The starting and the ending years should be numbers in the format yyyy." = 
#            !is.null(input_years))
# No longer needed because years are now numeric.

# Filter out years for which countries do not exist 
#is.country <- faoswsUtil::countryExists(countries = d_idrssr$geographicAreaM49,
#                                        years = d_idrssr$timePointYears)
#ind_idrssr <- d_idrssr[is.country]
# No longer needed because now we have countryReports().

# Check whether there are NaN values or not
#res <- sapply(ind_idrssr$idr, is.nan)
#res <- sapply(ind_idrssr$ssr, is.nan)
#sum(res)
#which(res)

## Print ending message ##
# Build the message's structure
# base_string <- paste("Process completed successfully!",
#                      "IDR indicator:",
#                      "%d written observations, %d appended, %d ignored, %d discarded.\n",
#                      "SSR indicator:",
#                      "%d written observations, %d appended, %d ignored, %d discarded.")
# 
# Fill in with results from SaveData() and return message
# message(do.call(sprintf, c(base_string,
#                            lapply(idr_save[1:4], FUN = identity),
#                            lapply(ssr_save[1:4], FUN = identity))))
