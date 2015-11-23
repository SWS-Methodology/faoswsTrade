# Extracts data from http://comtrade.un.org/db/mr/rfReportersList.aspx

library(dplyr)

url <- "http://comtrade.un.org/db/mr/rfReportersList.aspx"

html <- xml2::read_html(url)

m49 <- plyr::llply(1:5, function(x)
  rvest::html_nodes(html,
             paste0("#dgPzReporters td:nth-child(", x, ")")))

meta <- m49[5]

m49 <- as_data_frame(lapply(m49[1:4],
                                  function(x) {
                                    x <- stringr::str_trim(rvest::html_text(x))
                                    nm <- make.names(tolower(x[1]))
                                    x <- x[-1]
                                    setNames(list(x), nm)
                                    }) %>% unlist(recursive = FALSE)
                      )

m49 <- m49 %>%
  mutate(
    abbr = stringr::str_replace_all(name, "^Abbreviation: |Full Name.*$", ""),
    flnm = stringr::str_replace_all(name, "^.*Full Name: |Description.*$", ""),
    desc = stringr::str_replace_all(name, "^.*Description: |Comment.*$", ""),
    cmnt = stringr::str_replace_all(name, "^.*Comment: |Type: .*$", ""),
    type = stringr::str_replace(name, "^.*Type: ", "")) %>%
  select(-name) %>%
  mutate_each(funs(ifelse(. %in% c("", "N/A"), NA, .)), iso, desc, cmnt, type) %>%
  mutate(
    iso2   = stringr::str_extract(iso, "^[A-Z]{2}"),
    iso3   = stringr::str_extract(iso, "[A-Z]{3}$"),
    strtyr = stringr::str_extract(valid.years, "^\\d{4}"),
    endyr  = stringr::str_replace(valid.years, "^.* - ", ""),
    endyr  = ifelse(endyr == "Now", NA, endyr)) %>%
  select(-valid.years, -iso) %>%
  mutate_each(funs(as.integer), code, strtyr, endyr)



