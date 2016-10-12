#' Get Tariff Line (TL) or Eurostat (ES) raw data.
#'
#' @param origin String: "TL" (Tariff Line) or "ES" (Eurostat).
#' @param year Numeric.
#' @param test Logical: FALSE (default), complete data; TRUE, test data.
#' @param local Logical: FALSE (default), download test; TRUE, local test data.
#' @return A data.table containing trade data.
#' @export

# XXX (Christian): it would probably be better to add the following
# parameters: "chapters", "reporters" and "partners"

getTradeData <- function(origin = NA, year = NA, test = FALSE,
                         local = FALSE) {

  if (missing(origin)) stop('"origin" ("TL" or "ES") is required.')

  if (missing(year)) stop('"year" is required.')

  if (origin!="TL" & origin!="ES") stop('Use origin=="TL" or origin=="ES".')

  if (!is.logical(test)) stop('"test" must be either TRUE or FALSE')

  if (!is.logical(local)) stop('"local" must be either TRUE or FALSE')

  if (test==TRUE & local==TRUE) {
    local_file <- file.path(SETTINGS$testdir,
                            paste0(tolower(origin), 'data_', year, '.rds'))
  }

  if (origin=="TL") {
    # Chapters were provided by team B/C
    if (test==TRUE) {
      if (local==TRUE & file.exists(local_file)) {
        readRDS(local_file)
      } else {
        faosws::ReadDatatable(paste0("ct_tariffline_unlogged_", year),
                      columns=c("rep", "tyear", "flow", "comm", "prt",
                                "weight", "qty", "qunit", "tvalue", "chapter"),
                      where = "(rep IN ('32','124','170','231', '251','356',
                                '699','376','404','466','516','620','704',
                                '798', '818','842','858','899','NA','175',
                                '250','254', '312','474','492','638','652',
                                '663','630','840', '850','762')
                               AND prt IN ('32','124','170','231','251','356',
                                '699','376','404','466','516','620','704',
                                '798', '818','842','899','NA','175','250',
                                '254','312', '474','492','638','652','663',
                                '630','840','850', '762'))
                               AND chapter IN ('01', '02', '03', '04', '05',
                                '06', '07', '08', '09', '10', '11', '12',
                                '13', '14', '15', '16', '17', '18', '19',
                                '20', '21', '22', '23', '24', '33', '35',
                                '38', '40', '41', '42', '43', '50', '51',
                                '52', '53')"
                     )
      }
    } else {
      faosws::ReadDatatable(paste0("ct_tariffline_unlogged_", year),
                    columns=c("rep", "tyear", "flow", "comm", "prt", "weight",
                              "qty", "qunit", "tvalue", "chapter"),
                    where = "chapter IN ('01', '02', '03', '04', '05', '06',
                            '07', '08', '09', '10', '11', '12',  '13', '14',
                             '15', '16', '17', '18', '19', '20', '21',
                             '22', '23', '24', '33', '35', '38', '40',
                             '41', '42', '43', '50', '51', '52', '53')"
                   )
    }
  } else if (origin=="ES") {
    if (test==TRUE) {
      if (local==TRUE & file.exists(local_file)) {
        readRDS(local_file)
      } else {
        faosws::ReadDatatable(paste0("ce_combinednomenclature_unlogged_",year),
                    columns = c("declarant", "partner", "product_nc", "flow",
                                "period", "value_1k_euro", "qty_ton",
                                "sup_quantity", "stat_regime"),
                    where = "declarant IN ('001','010')
                             AND partner IN ('NA','001','0001','010','0010',
                              '0528','0404','0480','0334','0377','0496','0458',
                              '0462','0372','0524','0645','0646','0648','0020',
                              '0031','0033','0051','0057','0059','0916','0917',
                              '0990','0999','1010','1011','0664','0624','0389',
                              '0690','0220','0400','0457','0232','0346','0807',
                              '0082')"
                   )
      }
    } else {
      faosws::ReadDatatable(paste0("ce_combinednomenclature_unlogged_", year),
                    columns = c("declarant", "partner", "product_nc", "flow",
                                "period", "value_1k_euro", "qty_ton",
                                "sup_quantity", "stat_regime")
                   )
    }
  }
}
