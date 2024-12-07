

################################################################################
#
# Type: plugin.
#
# Name: ti_RCA.
#
# Description: R script to calculate the Revealed Comparative Advantage index
#              (RCA) starting from elements 5622 (Import Value) and 5922 (Export
#              Value. The following is a brief description of the indicator.
#              RCA: proxy for the country’s export potential. The RCA indicates
#                   whether a country is expanding the range of products in 
#                   which it possesses trade potential, as opposed to situations
#                   where the number of products that can be competitively 
#                   exported remains static.
#
# Author's email: riccardo.giubilei@fao.org
#
################################################################################



# Plugin initialization ---------------------------------------------------

# Plugin name
plugin_name <- 'ti_RCA'

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

# Add agricultural aggregates
cpc_codes <- c('F1881', 'F1882', cpc_codes)


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

# Print input parameters message
message(paste("The", plugin_name, "plugin is reading the input parameters."))

### Input domain ###

# Retrieve parameters' values
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

# Trade dataset: Import Value (5622) and Export Value (5922) of key commodities
#                and agricultural products F1881 and F1882
data_agrtrade <- sws2dataset(dataset = trade_dataset,
                             keys = list(geo = m49_codes,
                                         elem = c('5622', '5922'),   
                                         item = cpc_codes,
                                         years = selected_years),
                             elem_colnames = c('import', 'export'))



# RCA index ---------------------------------------------------------------

# Print indicator-calculation message
message(paste("The", plugin_name, "plugin is computing the indicators."))

# Filter out years for which countries do not report
is.reporter <- countryReports(countries = data_agrtrade$geographicAreaM49,
                              years = data_agrtrade$timePointYears)
data_agrtrade <- data_agrtrade[is.reporter]

# Compute world total exports
data_agrtrade[, world_export := sum(export, na.rm = TRUE),
              by = .(measuredItemCPC, timePointYears)]

# Convert data into a wide dataset to get data for agricultural aggregates
aggr_data <- dcast(data = data_agrtrade,
                   formula = geographicAreaM49 + timePointYears ~ measuredItemCPC,
                   subset = .(measuredItemCPC == 'F1881' | measuredItemCPC == 'F1882'),
                   value.var = c('export', 'world_export'))

# Omit agricultural aggregates to obtain data for key commodities
key_data <- data_agrtrade[measuredItemCPC != 'F1881' & measuredItemCPC != 'F1882',
                          .(geographicAreaM49, timePointYears, measuredItemCPC, export, world_export)]

# Final RCA data
data_rca <- merge(x = key_data,
                  y = aggr_data,
                  all = T)

# Revealed Comparative Advantage index for agricultural aggregate F1881
data_rca[, rca_f1881 := ((export / world_export) / (export_F1881 / world_export_F1881))]

# Make cases where the denominator is zero return NA
data_rca[world_export == 0 | export_F1881 == 0, 'rca_f1881'] <- NA_real_

# Revealed Comparative Advantage index for agricultural aggregate F1882
data_rca[, rca_f1882 := ((export / world_export) / (export_F1882 / world_export_F1882))]

# Make cases where the denominator is zero return NA
data_rca[world_export == 0 | export_F1882 == 0, 'rca_f1882'] <- NA_real_



# Save back the results and finalize --------------------------------------

# Print save message
message(paste("The", plugin_name, "plugin is saving the results back to the session."))

# If query_elem does not exist, initialize it
if (isFALSE(exists('query_elem'))) query_elem <- '0'

# Throw warning if none of the indicators is included in the queried elements
select_elem <- intersect(c('509.01', '509.02'),
                         query_elem)
nelem <- length(select_elem)
if (nelem == 0)
  warning('The indicators are not included in the queried key(s) for measuredElementTrade. Both of them will be returned.')

# Check which indicator(s) should be computed
compute_rca_f1881 <- if ('509.01' %in% select_elem | nelem == 0) TRUE else FALSE
compute_rca_f1882 <- if ('509.02' %in% select_elem | nelem == 0) TRUE else FALSE

# RCA - F1881
if (isTRUE(compute_rca_f1881)) {
  
  # Save the results back to the session
  rca_f1881_save <- 
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators',
                     data = data_rca[, .(geographicAreaM49 = geographicAreaM49,
                                            measuredItemCPC = measuredItemCPC,
                                            timePointYears = timePointYears,
                                            Value = rca_f1881,
                                            measuredElementTrade = '509.01',
                                            flagObservationStatus = fifelse(is.na(get('rca_f1881')),
                                                                            NA_character_,
                                                                            'E'),
                                            flagMethod = fifelse(is.na(get('rca_f1881')),
                                                                 NA_character_,
                                                                 'i'))],
                     waitTimeout = 100000)
  
  # Log
  base_string <- paste("Log for the RCA - F1881 indicator:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(rca_f1881_save[1:4], FUN = identity))))
  
}

# RCA - F1882
if (isTRUE(compute_rca_f1882)) {
  
  # Save the results back to the session
  rca_f1882_save <- 
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators',
                     data = data_rca[, .(geographicAreaM49 = geographicAreaM49,
                                            measuredItemCPC = measuredItemCPC,
                                            timePointYears = timePointYears,
                                            Value = rca_f1882,
                                            measuredElementTrade = '509.02',
                                            flagObservationStatus = fifelse(is.na(get('rca_f1882')),
                                                                            NA_character_,
                                                                            'E'),
                                            flagMethod = fifelse(is.na(get('rca_f1882')),
                                                                 NA_character_,
                                                                 'i'))],
                     waitTimeout = 100000)
  
  # Log
  base_string <- paste("Log for the RCA - F1882 indicator:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(rca_f1882_save[1:4], FUN = identity))))
  
}

# Print information
#print(sessionInfo())
#print(version)

# Print ending message 
message(paste("Process completed successfully!"))

