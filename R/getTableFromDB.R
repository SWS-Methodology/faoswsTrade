#' @import dplyr
#' @export

getTableFromDB <- function(
  sqlrequest,
  dbname = "sws_data",
  host = "hqldvpostgres01.hq.un.fao.org",
  port = 5420,
  user = "sws",
  password,
  options = "-c search_path=ess") {

  if(missing(password) & !exists(".swsdbpwd"))
    stop("Please provide password to db: as argument either create .swsdbpwd variable")

  if(missing(password) & exists(".swsdbpwd"))
    password <- .swsdbpwd

  sws_src <- src_postgres(dbname = dbname,
                            host = host,
                            port = port,
                            user = user,
                            password = password,
                            options = options)

  tbl_src <- tbl(sws_src, sql(sqlrequest))

  localtbl <- collect(tbl_src)

  RODBC::odbcClose(src_src$con)

  localtbl

}
