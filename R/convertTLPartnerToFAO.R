#' Convert tariffline partner code to FAO code
#'
#' @param partners Partners.
#'
#' @import dplyr
#' @export

convertTLPartnerToFAO <- function(partners) {
  partners <- data.frame(partner = as.integer(partners))

  data("FAOcountryProfile",
       package = "FAOSTAT",
       envir = environment())

  uniq_partners <- partners %>%
    distinct()

  uniq_partners <- uniq_partners %>%
    left_join(FAOcountryProfile %>%
                dplyr::select(un = UN_CODE,
                       fao = FAOST_CODE),
              by = c("partner" = "un"))

  uniq_partners$fao[uniq_partners$partner == 10] <- 30 # Antarctica
  uniq_partners$fao[uniq_partners$partner == 74] <- 31 # Bouvet Island
  uniq_partners$fao[uniq_partners$partner == 239] <- 271 # South Georgia and the South Sandwich Islands
  uniq_partners$fao[uniq_partners$partner == 473] <- NA # Latin American Integration Association, nes. NO FAO Code
  uniq_partners$fao[uniq_partners$partner == 488] <- 139 # Midway
  uniq_partners$fao[uniq_partners$partner == 490] <- NA # Other Aisa, nes. NO FAO
  uniq_partners$fao[uniq_partners$partner == 527] <- NA # Other Oceania, nes
  uniq_partners$fao[uniq_partners$partner == 568] <- NA # Other Europe, nes
  uniq_partners$fao[uniq_partners$partner == 577] <- NA # Other Africa, nes
  uniq_partners$fao[uniq_partners$partner == 637] <- NA # North America and Central America, nes
  uniq_partners$fao[uniq_partners$partner == 699] <- 100 # IN FAOSTAT It's 356 (excluding Sikkia). Change it!
  uniq_partners$fao[uniq_partners$partner == 711] <- NA # Southern African Customs Union
  uniq_partners$fao[uniq_partners$partner == 837] <- NA # Bunkers
  uniq_partners$fao[uniq_partners$partner == 838] <- NA # Free Zones
  uniq_partners$fao[uniq_partners$partner == 839] <- NA # Special categories
  uniq_partners$fao[uniq_partners$partner == 849] <- NA # US Misc. Pacific Isds
  uniq_partners$fao[uniq_partners$partner == 899] <- NA # Areas, nes


  # Nonmapped M49 partner codes: 251, 381,
  # 579, 581, 711, 757,  842

  # Switzerland (757) = Liechtenstein (438) + Switzerland (756)
  uniq_partners$fao[uniq_partners$partner %in% c(757)] <- 211
  # uniq_partners$fao[uniq_partners$partner %in% c(438, 756, 757)] <- 211

  # United States Minor Outlying Islands (581) =
  # Midway (488) + US Misc. Pacific Islands (849) + Wake Island (872)
  ### No in TL 2011 reporters, but in partners
  ### In FAO profiles Midway is part of 840 (the US)
  ##### So everything here should be 232 if to follow Comtrade
  # uniq_partners$fao[uniq_partners$partner == 488] <- 139 ## ALREADY DONE ABOVE
  uniq_partners$fao[uniq_partners$partner == 581] <- 232 ## US Minor Is. in FAO country profile
  # uniq_partners$fao[uniq_partners$partner == 849] <- NA
  # uniq_partners$fao[uniq_partners$partner == 872] <- 242 ## Wake Island in FAO country profile

  # USA (842) = Puerto Rico (630) + United States (840) + US Virgin Islands (850)
#   uniq_partners$fao[uniq_partners$partner == 630] <- 231 # Should be FAO 177
#   uniq_partners$fao[uniq_partners$partner == 840] <- 231
#   uniq_partners$fao[uniq_partners$partner == 850] <- 231 # Should be FAO 240
  uniq_partners$fao[uniq_partners$partner == 842] <- 231

  # France (251) = Mayotte (175), France (250), French Guiana (254),
  # Guadeloupe (312), Martinique (474), Monaco (492), Réunion (638),
  # Saint-Barthélemy (652), Saint-Martin (French part) (663)
  uniq_partners$fao[uniq_partners$partner == 251] <- 68

  # Norway (579) = Norway, excl. Bouvet Island, Svalbard & Jan Mayen Islands (578)+
  # Svalbard and Jan Mayen Islands (744)
  uniq_partners$fao[uniq_partners$partner == 579] <-162

  # Taiwan, Province of China (158)  is mapped to 490 (Asia, NES) in Comtrade dissemination.
#   > c(158, 490) %in% as.integer(tldata$reporter)
#   [1]  TRUE FALSE
#   > c(158, 490) %in% as.integer(tldata$partner)
#   [1]  TRUE FALSE

  # Italy
  uniq_partners$fao[uniq_partners$partner == 381] <-106


  partners <- partners %>%
    left_join(uniq_partners,
              by = "partner")

  partners$fao
}
