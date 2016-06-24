# NOTE: faosws required and a session already established

# Read in new, corrected cpc conversion file
fcl_2_cpc2_complete <- XLConnect::readWorksheetFromFile("data-raw/Conv_3DEC2015_E1_Simplified.xlsx",
                                                        sheet = "Sheet3")

# Convert to code on SWS system
colnames(fcl_2_cpc2_complete) <- c("fcl", "fcl_cpc_parlink", "cpc",
                                   "cpc_fcl_parlink", "description",
                                   "suafbs_convfact", "suafbs_notes",
                                   "fclcpc_convfact", "production_notes")

# Convert code meaning NA to NA
fcl_2_cpc2_complete$fcl[grep("n/a", fcl_2_cpc2_complete$fcl, ignore.case = TRUE)] <- NA

# Read in old file
final_destination = "fcl2cpc_ver_2_1"
table <- ReadDatatable(final_destination, readOnly = F)

# Delete all old values in table
changeset <- Changeset(final_destination)
AddDeletions(changeset, table)
Finalise(changeset)

# Replace with new values
AddInsertions(changeset, fcl_2_cpc2_complete)
Finalise(changeset)
