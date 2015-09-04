convertTLParnterToFAO <- function(partners) {
  partners <- data.frame(partner = as.integer(partners))

  data("FAOcountryProfile",
       package = "FAOSTAT",
       envir = environment())

  uniq_partners <- partners %>%
    distinct()

  uniq_partners <- uniq_partners %>%
    left_join(FAOcountryProfile %>%
                select(un = UN_CODE,
                       fao = FAOST_CODE),
              by = c("partner" = "un"))

  uniq_partners$fao[uniq_partners$partner == 10] <- 30 # Antarctica
  uniq_partners$fao[uniq_partners$partner == 74] <- 31 # Bouvet Island
  uniq_partners$fao[uniq_partners$partner == 239] <- 271 # South Georgia and the South Sandwich Islands
  uniq_partners$fao[uniq_partners$partner == 473] <- NA # Latin American Integration Association, nes. NO FAO Code
  uniq_partners$fao[uniq_partners$partner == 488] <- 139 # Midway
  uniq_partners$fao[uniq_partners$partner == 490] <- NA # Other Aisa, nes. NO FAO
  uniq_partners$fao[uniq_partners$partner == 527] <- NA # Other Oceania, nes
  uniq_partners$fao[uniq_partners$partner == 568] <- NA # Onter Europe, nes
  uniq_partners$fao[uniq_partners$partner == 577] <- NA # Other Africa, nes
  uniq_partners$fao[uniq_partners$partner == 637] <- NA # North America and Central America, nes
  uniq_partners$fao[uniq_partners$partner == 699] <- 100 # IN FAOSTAT It's 356 (excluding Sikkia). Change it!
  uniq_partners$fao[uniq_partners$partner == 837] <- NA # Bunkers
  uniq_partners$fao[uniq_partners$partner == 838] <- NA # Free Zones
  uniq_partners$fao[uniq_partners$partner == 839] <- NA # Special categories
  uniq_partners$fao[uniq_partners$partner == 849] <- NA # US Misc. Pacific Isds
  uniq_partners$fao[uniq_partners$partner == 899] <- NA # Areas, nes

  partners <- partners %>%
    left_join(uniq_partners,
              by = "partner")

  partners$fao
}
