#' Download raw Tariff-line data
#'
#' @export

getRawAgriTL <- function(year, agricodeslist) {
  tlsql <- paste0("
select * from (
select rep::int as reporter,
prt::int as partner,
comm as hs,
substring(comm from 1 for 6) as hs6,
flow::int,
tyear::int as year,
tvalue as value,
weight,
qty,
qunit::int
from ess.ct_tariffline_adhoc_unlogged) tbl1
where hs6 in (",
                  agricodeslist,
                  ") and year = ",
                  year)

  tldata <- getTableFromDB(tlsql)

  tldata$hs <- stringr::str_extract(tldata$hs, "^[0-9]*") # Artifacts in reporters 646 and 208

  tldata
}
