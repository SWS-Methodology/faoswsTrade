#' Reads a specific table from an MDB file, prepared for
#' Jellyfish software.
#'
#' It is expected folder name and file name are the same and
#' are formed from country name abbreviation and year, for example,
#' USA_2011.
#'

readMDBTable <- function(area,
                         year,
                         table,
                         mdbdir = c("",
                                    "mnt",
                                    "essdata",
                                    "TradeSys",
                                    "TradeSys",
                                    "Countries"),
                         colClasses = "character") {
  area <- toupper(area)

  if(length(mdbdir) > 1) mdbdir <- do.call("file.path", as.list(mdbdir))

  dirname <- paste0(area, "_", year)

  path <- file.path(mdbdir, dirname)

  if(!file.exists(path)) stop(paste0("Directory ",
                                     path,
                                     " does not exist."))

  filepath <- file.path(path, paste0(dirname, ".mdb"))

  if(!file.exists(filepath)) stop(paste0("File ",
                                     filepath,
                                     " does not exist."))


  Hmisc::mdb.get(filepath,
                 colClasses = "character",
                 stringsAsFactors = F,
                 lowernames = T,
                 tables = table)


}
