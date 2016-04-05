#' Download raw EuroStat data
#'
#' @export

getRawAgriES <- function(year, agricodeslist) {

  essql <- paste0("
select * from (
select declarant::int as reporter,
partner::int,
product_nc as hs,
substring(product_nc from 1 for 6) as hs6,
flow::int,
substring(period from 1 for 4)::int as year,
value_1k_euro as value,
qty_ton as weight,
sup_quantity as qty
from ess.ce_combinednomenclature_unlogged
where declarant <> 'EU') tbl1
where hs6 in (",
                  agricodeslist,
                  ") and year = ",
                  year)

  getTableFromDB(essql)
}
