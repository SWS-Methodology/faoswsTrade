fcl_2_cpc2_complete <- XLConnect::readWorksheetFromFile("data-raw/Conv_3DEC2015_E1_Simplified.xlsx",
                                                        sheet = "Sheet3")

final_destination = "fcl2cpc_ver_2_1"
table <- ReadDatatable(final_destination, readOnly = F)

colnames(fcl_2_cpc2_complete) <- c("fcl", "fcl_cpc_parlink", "cpc",
                                   "cpc_fcl_parlink", "description",
                                   "suafbs_convfact", "suafbs_notes",
                                   "fclcpc_convfact", "production_notes")

fcl_2_cpc2_complete$fcl[grep("n/a",fcl_2_cpc2_complete$fcl)] <- NA

changeset <- Changeset(final_destination)
AddDeletions(changeset, table)
Finalise(changeset)

AddInsertions(changeset, fcl_2_cpc2_complete)
Finalise(changeset)
