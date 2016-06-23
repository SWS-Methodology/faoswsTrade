fcl_2_cpc2_complete <- XLConnect::readWorksheetFromFile("data-raw/Conv_3DEC2015_E1_Simplified.xlsx",
                                                        sheet = "Sheet3")

final_destination = "fcl2cpc_ver_2_1"
table <- ReadDatatable(final_destination, readOnly = F)

changeset <- Changeset(final_destination)
AddDeletions(changeset, table)
Finalise(changeset)

AddInsertions(changeset, fcl_2_cpc2_complete)
Finalise(changeset)


#fcl_2_cpc2 <- fcl_2_cpc2_complete[, colnames(fcl_2_cpc2_complete)[c(1,3)]]
#colnames(fcl_2_cpc2) <- c("fcl","cpc")
#fcl_2_cpc2$fcl[grepl("n/a",fcl_2_cpc2$fcl)] <- NA

#write.csv(fcl_2_cpc2, file = "fcl_2_cpc2.csv", quote = T, row.names = F)
#save(fcl_2_cpc2, file = "data/fcl_2_cpc2.RData")
