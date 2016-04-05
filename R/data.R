#' UNSD partner reference list
#'
#' Taken from Melissa Paca paca@un.org
#'
#' @format A data frame

"unsdpartners"

#' UNSD partner blocksreference list
#'
#' Taken from Melissa Paca paca@un.org
#'
#' @format A data frame

"unsdpartnersblocks"

#' Units of measurment for FCL codes
#'
#' Taken as XLS from Claudia DeVita Claudia.DeVita@fao.org
#'
#' @format A data frame with two columns: integer FCL code
#' and character description of a unit

"fclunits"

#' Units of measuremnt for Comtrade datasets
#'
#' Taken from Annex I of
#' http://unstats.un.org/unsd/tradekb/Knowledgebase/Quantity-and-Weight-Data-in-UN-Comtrade
#'
#' @format A data frame: qunit (integer) UN Comtrade Code,
#' wco (character) - WCO Abbreviation,
#' desc (character) - Description

"comtradeunits"

#' Eurostat Geonomenclature to FAO area codes mapping
#'
#' Extracted from CountryCode table of FRA_2012.mdb located in
#'  /mnt/essdata/TradeSys/TradeSys/Countries/FRA_2012/
#' All EU countries have similar codes. So we took France.
#' Idea suggested by Claudia
#'
#' @format A data frame: code (integer) Geonom code,
#' faostat (integer) - FAO area code,
#' active (integer) - FAO area code to use. For example, French Guiana
#' goes to France. So we don't need to aggregate later (probably). Abandoned codes,
#' like USSR, in active column were equal 0, but replaced by NA.

"geonom2fao"


#' Mapping between character codes of areas in folders' names with
#' Jellyfish MDB files and FAO area codes
#'
#' Produced by Onno
#'
#' @format A data frame:
#' mdbarea - character vector with area codes in folders with MDB
#' faoarea - integer vector with FAO area codes

"mdbfaoareamap"

#' Table of M49 area codes. Taken from
#' http://comtrade.un.org/db/mr/rfReportersList.aspx and 652 added manually.
#'

"m49"

#' Comtrade M49 <-> FAO area list map.
#' This table should be used for reporters codes.

"m49faomap"

#' Local replacement for table with fcl codes and descriptions from SWS
#' @format data.table
"fclcodelist"

#' Valid dataset from database to be compared with new procedure
#'
#' Taken as CSV from Giorgio Sorbara Giorgio.Sorbara@fao.org
#'
#' @format A data frame with seven columns as integers: year, reporter,
#'  partner, fcl, quantity and value

"tfvalid"


#' Information of country code and names
#'
#' Taken from data_raw, put by Alex
#'
#' @format A data frame with two columns as integer and character:
#' reporter and country_name

"fao_country_code"
