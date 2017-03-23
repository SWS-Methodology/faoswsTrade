library(magrittr)
# Read HS6 codes of interest from Claudia's Excel file

hs6faointerest <- XLConnect::readWorksheetFromFile(
  file.path("data-raw",
            "HSagric_filter.xls"),
  header = TRUE,
  startCol = 1L,
  endCol = 2L,
  startRow = 1L,
  endRow = 43L,
  sheet = "Sheet1")

hs6faointerest <- unname(unlist(plyr::alply(hs6faointerest, 1L, function(i)
  seq.int(from = i$FromCode, to = i$ToCode))))

# Convert to character and adding leading zero for 5-digit length codes to
# make it similar to previous version of the filter
hs6faointerest <- sprintf("%06d", hs6faointerest)


save(hs6faointerest,
     file = file.path("data", "hs6faointerest.RData"))

