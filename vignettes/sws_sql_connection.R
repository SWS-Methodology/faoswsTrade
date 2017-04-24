.swsdbpwd <- "PWD-GOES-HERE"

trade_src <- src_postgres(dbname = "sws_data",
                          host = "hqldvpostgres01.hq.un.fao.org",
                          port = 5420,
                          user = "sws",
                          password = .swsdbpwd)

# It is required for table view
# ,
#                           options = "-c search_path=ess")

agri_db <- tbl(trade_src, sql("
                              select * from ess.ce_combinednomenclature_unlogged
                              limit 10"))


