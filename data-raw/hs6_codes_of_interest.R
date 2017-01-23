library(magrittr)
# Read HS6 codes of interest from Claudia's Excel file

hs6faointerest <-  XLConnect::readWorksheetFromFile(
  file.path("data-raw",
            "HS2012-6 digits Standard.xls"),
  header = FALSE,
  startCol = 1L,
  endCol = 1L,
  startRow = 2L,
  # endRow = 1062L,
  sheet = "Standard_HS12") %>% unlist %>% unname %>%
  stringr::str_extract("^\\d{6}") # There is one 7-digit code 0207160 - we trim it
# Yep, Claudia confirmed:

# ----- Message from "Claudia (ESS)" <@fao.org> ---------
# Дата: Mon, 14 Nov 2016 12:58:21 +0000
# От кого: "Claudia (ESS)" <@fao.org>
# Тема: RE: Unmatched CN8 codes: 01051500 for Austria and Greece
# Кому: Alexander Matrunich
#
#
# > Yes, it should be 020716.
# >
# > -----Original Message-----
# > From: Alexander Matrunich
# > Sent: 14 November 2016 12:52 PM
# > To: Claudia (ESS)
# > Cc:  Carola (ESS)
# > Subject: Re: Unmatched CN8 codes: 01051500 for Austria and Greece
# >
# > Wow, that is very useful: we can increase speed of mapping process by
# > removing such codes from original trade data sets before the mapping.
# >
# > There is one code of length 7 in the table: 0207160 1059 OFFALS LIVER CHICKEN
# >
# > Is it safe to trim it to 020716?
# >
# > A.

save(hs6faointerest,
     file = file.path("data", "hs6faointerest.RData"))
