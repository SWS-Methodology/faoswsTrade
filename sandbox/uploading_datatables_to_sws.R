library(data.table)

ts_all_reports(
  collection_path = "//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/pre_processing_report",
  # collection_path = "C:/Users/caetano/Documents/trade-ad-hoc/running_module_2009-15_2017_07_03",
  prefix = "complete_tf_cpc", complete = FALSE
)

# tab 1
tab1 = fread("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/pre_processing_report/ts_reports/Table_1_ts_reporters.csv")
tab1[, list(length(unique(rep_code))), by = rep_name][order(-V1)]
tab1[rep_name == "Croatia"]
names(tab1)[3:18] <- paste0("year_", names(tab1)[3:18])
## Delete
table = "reporters_years"
changeset <- Changeset(table)
newdat <- ReadDatatable(table, readOnly = FALSE)
AddDeletions(changeset, newdat)
Finalise(changeset)
## Add
table = "reporters_years"
changeset <- Changeset(table)
AddInsertions(changeset, tab1)
Finalise(changeset)

# tab 2
tab2 = fread("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/pre_processing_report/ts_reports/Table_2_ts_non_reporters.csv")
tab2[, list(length(unique(non_rep_area))), by = non_rep_name][order(-V1)]
tab2[non_rep_name == "Croatia"]
names(tab2)[3:18] <- paste0("year_", names(tab2)[3:18])
## Delete
table = "non_reporting_countries"
changeset <- Changeset(table)
newdat <- ReadDatatable(table, readOnly = FALSE)
AddDeletions(changeset, newdat)
Finalise(changeset)
## Add
table = "non_reporting_countries"
changeset <- Changeset(table)
AddInsertions(changeset, tab2)
Finalise(changeset)

# tab 3
tab3 = fread("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/pre_processing_report/ts_reports/Table_3_ts_preproc_long_format.csv")
tab3[, list(length(unique(rep_code))), by = rep_name][order(-V1)]
## Delete
table = "numb_records_reporter"
changeset <- Changeset(table)
newdat <- ReadDatatable(table, readOnly = FALSE)
AddDeletions(changeset, newdat)
Finalise(changeset)
## Add
table = "numb_records_reporter"
changeset <- Changeset(table)
AddInsertions(changeset, tab3)
Finalise(changeset)

# tab 4
tab4 = fread("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/pre_processing_report/ts_reports/Table_4_ts_check_content.csv")
tab4[, list(length(unique(rep_code))), by = rep_name][order(-V1)]
## Delete
table = "imp_exp_content_check"
changeset <- Changeset(table)
newdat <- ReadDatatable(table, readOnly = FALSE)
AddDeletions(changeset, newdat)
Finalise(changeset)
## Add
table = "imp_exp_content_check"
changeset <- Changeset(table)
AddInsertions(changeset, tab4)
Finalise(changeset)

# tab 5
tab5 = fread("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/pre_processing_report/ts_reports/Table_5_ts_flows_check.csv")
tab5[, list(length(unique(rep_code))), by = rep_name][order(-V1)]
## Delete
table = "qty_values_country_flow_year"
changeset <- Changeset(table)
newdat <- ReadDatatable(table, readOnly = FALSE)
AddDeletions(changeset, newdat)
Finalise(changeset)
## Add
table = "qty_values_country_flow_year"
changeset <- Changeset(table)
AddInsertions(changeset, tab5)
Finalise(changeset)


# tab 6
tab6 = fread("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/pre_processing_report/ts_reports/Table_6_ts_missing_records.csv")
tab6[, list(length(unique(rep_code))), by = rep_name][order(-V1)]
## Delete
table = "report_missing_data"
changeset <- Changeset(table)
newdat <- ReadDatatable(table, readOnly = FALSE)
AddDeletions(changeset, newdat)
Finalise(changeset)
## Add
table = "report_missing_data"
changeset <- Changeset(table)
AddInsertions(changeset, tab6)
Finalise(changeset)

update_ppr <- function(data) {



}


