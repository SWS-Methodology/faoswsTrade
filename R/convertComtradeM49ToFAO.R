#' Converts area codes, stored in UNSD Comtrade M49, into FAO area codes
#'
#' @param Numeric or character vector of UNSC M49 area codes
#'
#' @return Interger vector of FAO area codes
#'
#' @import dplyr
#' @export

convertComtradeM49ToFAO <- function(reporters) {

  if(!is.integer(reporters)) reporters <- as.integer(reporters)

  reporters <- data.frame(reporter = reporters,
                          stringsAsFactors = F)

  uniq_reporters <- reporters %>%
    distinct_() %>%
    left_join(getComtradeM49() %>%
                mutate_(code = ~as.integer(code)),
              by = c("reporter" = "code")) %>%
    mutate_(faoarea = ~countrycode::countrycode(name, "country.name", "fao",
                                              warn = F))

  uniq_reporters$faoarea[uniq_reporters$name == "State of Palestine"] <- 299 # Wrong regex in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "New Caledonia"] <- 153 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "Greenland"] <- 85 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "Mayotte"] <- 270 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "Bermuda"] <- 17 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "French Polynesia"] <- 70 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "Turks and Caicos Isds"] <- 224 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "Aruba"] <- 22 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "Montserrat"] <- 142 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "China, Hong Kong SAR"] <- 96 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "China, Macao SAR"] <- 128 # No code for FAO in countrycode
  uniq_reporters$faoarea[uniq_reporters$name == "China"] <- 41 # In countrycode it is linked to China including Hong Kong and Macao

  if(any(is.na(uniq_reporters$faoarea))) warning("Some area codes were not recognized")

  reporters <- reporters %>%
    left_join(uniq_reporters,
              by = "reporter")

  reporters$faoarea

}
