# MDB area codes to FAO area codes by Onno

# Original code was taken from hsfclmap package


mdbfaoareamap <- XLConnect::readWorksheetFromFile(file.path("data-raw",
                                                    "Trade_Country-Registry.xlsx"),
                                          header = T,
                                          sheet = "Sheet1",
                                          endCol = 2)
mdbfaoareamap$Acronyme <- toupper(mdbfaoareamap$Acronyme)
names(mdbfaoareamap) <- c("mdbarea", "faoarea")
mdbfaoareamap$faoarea <- as.integer(mdbfaoareamap$faoarea)
mdbfaoareamap <- rbind(mdbfaoareamap,
                       data.frame(mdbarea = "VIE",
                                  faoarea = 237L,
                                  stringsAsFactors = F)
                       )

save("mdbfaoareamap", file = file.path("data", "mdbfaoareamap.RData"))
