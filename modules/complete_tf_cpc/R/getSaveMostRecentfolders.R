#' Get and save the most updated folders.
#'
#' This function selects the most updated trade folders and copies specific files
#' to another directory. The files copied are: development.log and the entire folder datadir.
#' The subfolder "datadir" is essential for creating the Pre-Processing Report.
#'
#'
#' @param initialDir Path where there are the trade folders.
#' @param saveDir Path where the most updated trade folders should be saved.
#' @param minYear Minimum year to be processed the Pre-Processing Report.
#' @param maxYear Maximum year to be processed the Pre-Processing Report.
#' @return Folders copied to a specific directory.
#' @import data.table
#' @export

getSaveMostRecentfolders <- function(initialDir = initial, saveDir = save,
                                     minYear = minYearToProcess, 
                                     maxYear = maxYearToProcess
                                     ) {
  
  unlink(save, recursive = T)
  dir.create(save)
  
  listFolders = file.info(list.dirs(initialDir, recursive=FALSE))
  if(length(listFolders) == 0) stop("There are no files present in the initial folder")
  
  folder = rownames(listFolders)
  listFolders <- as.data.table(listFolders)
  listFolders = cbind(folder, listFolders)  
  listFolders[, flagPPR := grepl("complete_tf_cpc_", folder)]
  listFolders = listFolders[flagPPR == T]
  listFolders[, yearTrade := stringr::str_extract(folder, "[[:digit:]]{4}(?=_master)")]
  
  
  listFolders = listFolders[with(listFolders, order(as.POSIXct(mtime))), ]
  listFolders = listFolders[!is.na(atime)]
  mostUpdatedOnes = listFolders[, list(time = max(ctime)),
                                list(yearTrade)]
  
  listFolders = listFolders[ctime %in% mostUpdatedOnes$time]
  listFolders[, yearTrade := as.numeric(yearTrade)]
  listFolders = listFolders[yearTrade != is.numeric(yearTrade)]
  
  if(min(listFolders$yearTrade) != minYear) 
    stop ('Mininum year available for computing the PPR is different from the min year requested') 
  
  if(max(listFolders$yearTrade) != maxYear) 
    stop ('Maximum year available for computing the PPR is different from the max year requested')
  
  if(maxYear - minYear + 1 != length(unique(listFolders$yearTrade)))
    stop ('At least one year is missing')
  
  for(i in 1:nrow(listFolders)) {
    
    subdirName = paste(save, basename(listFolders$folder[i]), sep = "/")
    dir.create(subdirName)
    file.copy(paste0(listFolders$folder[i], "/datadir"), subdirName, recursive = T)
    file.copy(paste0(listFolders$folder[i], "/development.log"), subdirName, recursive = T)
  }
}

