###########################################################################
######## This scripts harvest trade data from UNSD and Eurostat ###########
###########################################################################

# The first part deals with UNSD Tariffline data: one SMDX file for each
# reporter and chapter will be downloaded in a loop, and all the files
# will then be combined in a single file. The second part is easier as
# it consists in just loading a single file for Eurostat data (with some
# small data manipulation)

# Required packages:
#
# "rsdmx"
# "data.table"
# "dplyr"
# "curl"
# "XML"
# "plyr"
# "parallel"
# "stringr"

# UPPERCASE variables are parameters.


# Eurostat file with all data for 1 year
# (downloaded from Eurostat's bulk download website)
EUROSTAT_DAT_FILE <- 'c:/Users/mongeau.FAODOMAIN/Downloads/full201752/full201752.dat'

# Set the folder where files will be downloaded.
# NOTE: The folder should be empty when the process starts.
# If it doesn't exist, it will be created.
DESTINATION_FOLDER <- "c:/Users/mongeau.FAODOMAIN/tmp/trade_data"

# If FORCE_DOWNLOAD = TRUE the file will be downloaded even
# if it already exists. Especially useful if a new version
# of data (which was already downloaded) is required. If set
# to FALSE, then it will avoid downloading existing files.
FORCE_DOWNLOAD <- FALSE

# If PARALLEL = TRUE parallel processing will be used (i.e.,
# the loop will use various cores in the machine so that the
# process will be faster). If FALSE, the process will be
# embarrassingly parallel (in techincal parlance).
PARALLEL <- FALSE

#################### REPORTERS ####################

# Set the reporters to download as a numeric vector of M49 codes.
# NOTE: Only non EU countries, as EU countries will ALL be downloaded
# from Eurostat.

#REPORTERS <- 170
#REPORTERS <- c(8, 12, 28, 32, 51, 533, 36, 31, 112, 84, 60, 70, 72, 76, 132, 152, 156, 344, 170, 61, 214, 218, 818, 268, 288, 328, 340, 352, 360, 7, 392, 400, 398, 414, 417, 466, 478, 480, 484, 499, 104, 516, 3, 554, 566, 579, 512, 586, 585, 608, 498, 643, 882, 678, 686, 694, 702, 90, 710, 740, 757, 764, 768, 788, 834)
#REPORTERS <- c(116, 136, 140, 158, 188, 20, 204, 231, 24, 258, 270, 275, 296, 304, 320, 368, 384, 4, 418, 422, 44, 446, 454, 462, 470, 48, 496, 50, 500, 504, 540, 562, 591, 634, 646, 662, 682, 704, 716, 776, 780, 784, 854, 887)

#REPORTERS <-
#  c(4, 8, 12, 20, 24, 28, 31, 32, 36, 40, 44, 48, 50, 51, 52, 56, 60, 64, 68, 70, 72, 76, 84, 90, 96, 97,
#    100, 104, 108, 112, 116, 120, 124, 132, 136, 140, 144, 152, 156, 158, 170, 174, 175, 178, 184, 188, 191, 192, 196,
#    203, 204, 208, 212, 214, 218, 222, 231, 233, 234, 242, 246, 251, 258, 262, 266, 268, 270, 275, 276, 288, 296,
#    300, 304, 308, 320, 324, 328, 340, 344, 348, 352, 360, 364, 368, 372, 376, 381, 384, 388, 392, 398,
#    400, 404, 410, 414, 417, 418, 422, 426, 428, 434, 440, 442, 446, 450, 454, 458, 462, 466, 470, 478, 480, 484, 490, 496, 498, 499,
#    500, 504, 508, 512, 516, 524, 528, 530, 533, 540, 548, 554, 558, 562, 566, 579, 583, 585, 586, 591, 598,
#    600, 604, 608, 616, 620, 624, 626, 634, 642, 643, 646, 659, 660, 662, 670, 678, 682, 686, 688, 690, 694, 699,
#    702, 703, 704, 705, 710, 716, 724, 729, 736, 740, 748, 752, 757, 760, 764, 768, 776, 780, 784, 788, 792, 796, 798,
#    800, 804, 807, 818, 826, 834, 842, 854, 858, 862, 876, 882, 887, 894
#  )

# Markie (Dominique's email on 2019-05-07)
REPORTERS <- c(842, 36, 8, 490) # 490 should be Taiwan

#################### YEARS #######################

# Set the year(s) as a numeric vector.
# NOTE: settimg multiple years is NOT tested, so it's
# better to process just one year at a time.

YEARS <- 2017

#################### COMMODITIES #######################

# Set the starting numerical codes of the commodities that are
# required as character vector. Use leading zero if needed.
# NOTE: An asterisk will be always added, i.e., 03*, 03229870*, etc.

#COMMODITIES <- stringr::str_pad(1:99, 2, 'left', 0)
#COMMODITIES <- c("03", "40")

COMMODITIES <-
  c("01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24",
    "33", "35", "38",
    "40", "41", "43",
    "50", "51", "52", "53"
  )


# The maximum number of seconds to wait an answer from the server.
# Change this if you know what you are doing.
TIMEOUT <- 1200


# The progress of the downloading process can be traced in the "log"
# folder located in DESTINATION_FOLDER (count the number of files).



















##########################################################################
##########################################################################
## END OF PARAMETERS FROM HERE YOU ARE NOT SUPPOSED TO CHANGE ANYTHING. ##
##########################################################################
##########################################################################


if (!dir.exists(DESTINATION_FOLDER)) {
  dir.create(DESTINATION_FOLDER)
}

setwd(DESTINATION_FOLDER)

library("rsdmx")
library("data.table")
library("dplyr")
library("curl")

options(timeout = TIMEOUT)


############################################################
######### UNSD #############################################
############################################################

REPORTERS <- as.character(REPORTERS)

api <- "https://comtrade.un.org/ws/getsdmxtarifflinev1.aspx?"

query_url <- function(reporter, year, commodity) {
  paste0(api, "px=*&p=*&comp=false", "&r=", reporter,
         "&y=", year, "&cc=", commodity, "*")
}

# If flow is used (an additional parameter is required):
#query_url <- function(reporter, year, commodity, flow) {
#  paste0(api, "px=*&p=*&comp=false", "&r=", reporter,
#         "&y=", year, "&rg=", flow, "&cc=", commodity, "*")
#}


# Extract number of observations available in SDMX
# (checked on CrossSectionalData)
n_obs <- function(x) {
  length(XML::getNodeSet(x, '//uncs:Obs'))
}

d <- expand.grid(year = YEARS, commodity = COMMODITIES, reporter = REPORTERS)

# Create destination folders, if not already existing
apply(d, 1,
  function(x) {
    if (!dir.exists(file.path(x[['year']], x[['reporter']]))) {
      dir.create(file.path(x[['year']], x[['reporter']]), recursive = TRUE)
    }
  }
)

sapply(c("final", "log", "bin"), function(x) if (!dir.exists(x)) dir.create(x))

sapply(
  YEARS,
  function(x) {
    if (!dir.exists(file.path("bin", x))) {
      dir.create(file.path("bin", x))
    }
  }
)


write(nrow(d), paste0('log/_n_lines.txt'))

if (PARALLEL == TRUE) {
  # Leave one core available
  n_cores <- parallel::detectCores() - 1

  cl <- parallel::makeCluster(n_cores)

  doParallel::registerDoParallel(cl)

  parallel::clusterExport(
    cl,
    c("query_url", "api", "FORCE_DOWNLOAD", "TIMEOUT")
  )

  parallel::clusterEvalQ(
    cl, {
      library("rsdmx")
      library("dplyr")
      library("curl")
      options(timeout = TIMEOUT)
      NULL
    }
  )
}

results <-
  plyr::ddply(
    .data = d,
    # XXX: something happens with 381: data is not downloaded (increase timeout?)
    #### "An error has occured, please contact comtrade@un.org"
    #.data = d[d$reporter == '381',],
    #.data = d[d$reporter == '76',],
    #.data = d[d$reporter == '156',],
    .variables = c("year", "reporter", "commodity"),
    .fun = function(x) {
      u <- query_url(x[["reporter"]], x[["year"]], x[["commodity"]])
      f <- file.path(x[["year"]], x[["reporter"]], paste0(x[["year"]], "_", x[["reporter"]], "_", x[["commodity"]], ".xml"))

      file.create(file.path("log", paste0(x[["year"]], "_", x[["reporter"]], "_", x[["commodity"]])))

      if (file.exists(f) && file.size(f) == 0) {
        unlink(f)
      }

      # Thing for download loop
      mytry <- try(log('xxx'), silent = TRUE) # fake error
      counter <- 1
      file_size <- 0

      if (FORCE_DOWNLOAD || !file.exists(f)) {

        # Will try max 3 times
        while((inherits("try-error", mytry) || file_size == 0) && counter < 3) {
          mytry <- try(curl_download(u, f, quiet = TRUE), silent = TRUE)
          if (file.exists(f)) {
            file_size <- file.size(f)
          }
          counter <- counter + 1
        }

        if (inherits("try-error", mytry)) {
          return(NA)
        } else {
          return(file.size(f))
        }
      } else {
        if (file.exists(f)) {
          return(file.size(f))
        } else {
          return(NA)
        }
      }
    },
    .parallel = PARALLEL,
    .progress = "text"
  ) %>%
  rename(result = V1)


# Check issues
down_issues <- plyr::ddply(
  results,
  .variables = c('year', 'reporter', 'commodity'),
  .fun = function(x) {
    f <- paste0(x$year, '/', x$reporter, '/', x$year, '_', x$reporter, '_', x$commodity, '.xml')
    if (x$result < 1000) {
      return(grepl('An error has occured', as.character(xml2::read_html(f))))
    } else {
      return(FALSE)
    }
  },
  .progress = 'text'
)


#### WILL SHOW REPORTERS WITH ISSUES IN DOWNLOADE FILES:
down_issues %>%
  group_by(reporter) %>%
  summarise(perc = sum(V1) / n()) %>%
  filter(perc > 0)



saveRDS(results, paste0("log/_results_", format(Sys.time(), "%Y%m%d%H%M"), ".rds"))




save_reporter_year_bin <- function(year_reporter) {
  myres <-
    plyr::mlply(
      #dir('2017', recursive = TRUE, full.names = TRUE),
      #dir('2016/842/', full.names = TRUE),
      #dir('2016/76/', full.names = TRUE),
      dir(year_reporter, full.names = TRUE),
      function(x) {
        invisible(gc())
        sdmx <- readSDMX(x, isURL = FALSE, verbose = FALSE)
        if (n_obs(sdmx@xmlObj) > 0) {
          dplyr::as_data_frame(sdmx)
        } else {
          NULL
        }
      },
      .progress = "none"
    )

  if (!all(sapply(myres, is.null))) {
    myres <- bind_rows(myres)

    if (!('value'     %in% names(myres))) myres$value     <- NA_real_
    if (!('netweight' %in% names(myres))) myres$netweight <- NA_real_
    if (!('qty'       %in% names(myres))) myres$qty       <- NA_real_
    if (!('REPORTED_CURRENCY' %in% names(myres))) myres$REPORTED_CURRENCY <- NA_character_

    myres <-
      myres %>%
      bind_rows() %>%
      select(
        rep     = RPT,
        tyear   = time,
        curr    = CURRENCY,
        hsrep   = REPORTED_CLASSIFICATION,
        flow    = TF,
        repcurr = REPORTED_CURRENCY,
        comm    = matches('^CC'), # Can be CC.H2, CC.H3, etc.
        prt     = PRT,
        weight  = netweight,
        qty     = qty,
        qunit   = QU,
        tvalue  = value,
        est     = EST,
        ht      = HT
        ) %>%
      mutate(
        chapter = stringr::str_sub(comm, 1, 2),
        tvalue  = as.numeric(tvalue),
        weight  = as.numeric(weight),
        qty     = as.numeric(qty)
        ) %>%
      select(chapter, everything())

    file_to_save <- paste0("bin/", year_reporter, ".rds")

    saveRDS(myres, file_to_save)

    rm(myres)

    invisible(gc())

    return(ifelse(file.exists(file_to_save), TRUE, FALSE))
  } else {
    return(NA)
  }

}









# NOTE: up to this point it can run by itself, but the process below needs
# to be carried out manually as memory is not released after files are run
# (so the RAM gets filled up constantly).






# XXX: the returned indices are not `year_reporter`
saved_data <-
  plyr::mdply(
  (expand.grid(YEARS, REPORTERS) %>% mutate(x = paste(Var1, Var2, sep = "/")))$x,
  #"2016/251",
  #dir("2017", full.names = TRUE),
  function(x) {
    #if (!file.exists(paste0("bin/", x, ".rds"))) {
      save_reporter_year_bin(x)
    #}
  },
  .progress = "text"
)


# TODO: generalise for multiple years.
tldata <-
  plyr::mdply(
    dir(paste0("bin/", YEARS), full.names = TRUE),
    readRDS,
    .progress = "text"
  ) %>%
  select(-X1) %>%
  setDT()

# If weight is zero or missing AND qty is available AND qunit == 8
# then we can assign qty to weight.

tldata[, weight := ifelse((weight < 0.0001 | is.na(weight)) & !is.na(qty) & qunit == '8', qty, weight)]

saveRDS(tldata, paste0("final/ct_tariffline_unlogged_", YEARS, ".rds"))




############################################################
#### checks ################################################
############################################################
#library(dplyr)
#
#files <- dir('c:/Users/mongeau.FAODOMAIN/raw_tl_data/2017', recursive = TRUE, full.names = TRUE)
#
#stats <-
#  plyr::mdply(files, file.size, .progress = 'text') %>%
#  tbl_df() %>%
#  mutate(X1 = files, base = basename(X1)) %>%
#  rename(size = V1, file = X1) %>%
#  tidyr::separate(base, into = c('year', 'reporter', 'chapter'), sep = '_') %>%
#  mutate(chapter = sub('.xml', '', chapter))



#############################################################
##!/usr/bin/bash
#
#for j in $(ls) ; do
#  cd $j
#  for i in $(ls) ; do
#    printf $i
#    sed 's/.*REPORTED_CLASSIFICATION="\(..\)".*/\1/' $i
#    echo
#    #echo XXX $i
#  done
#  cd ..
#done






############################################################
####### EUROSTAT ###########################################
############################################################


esdata <- fread(EUROSTAT_DAT_FILE, colClasses = "character")

names(esdata) <- tolower(names(esdata))

# That should be it
esdata[supp_unit == "", sup_quantity := NA_character_]


esdata <-
  esdata[,
    list(
      period,
      declarant,
      partner,
      flow,
      product_nc,
      value_1k_euro = as.numeric(value_in_euros) / 1000,
      qty_ton = as.numeric(quantity_in_kg) / 1000,
      sup_quantity = as.numeric(sup_quantity),
      stat_regime,
      supp_unit
    )
  ]

esdata <- esdata[!grepl('[A-Z]', product_nc)]

esdata <-
  esdata[,
    `:=`(
      value_1k_euro = sum(value_1k_euro),
      qty_ton = sum(qty_ton),
      sup_quantity = sum(sup_quantity)
    ),
    list(
      period,
      declarant,
      partner,
      flow,
      product_nc
    )
  ]

#esdata[, .N, list(period, declarant, partner, flow, product_nc, stat_regime)]


#esdata <- esdata[substr(esdata$product_nc, 1, 2) %in% unique(substr(esdata$product_nc, 1, 2))]

esdata[, supp_unit := NULL]
esdata[, stat_regime := "4"]

# ???????????????????????????????????
#esdata[, `:=`(value_1k_euro = sum(value_1k_euro), qty_ton = sum(qty_ton), sup_quantity = sum(sup_quantity)), list(period, declarant, partner, flow, product_nc)] %>%
esdata <- esdata %>%
  tbl_df() %>%
  group_by(period, declarant, partner, flow, product_nc) %>%
  summarise(value_1k_euro = first(value_1k_euro), qty_ton = first(qty_ton), sup_quantity = first(sup_quantity)) %>%
  ungroup() %>%
  mutate(stat_regime = "4") %>%
  setDT()


saveRDS(esdata, "final/ce_combinednomenclature_unlogged_2017.rds")























##library(dplyr)
##
##tps <- tibble::tribble(
##~year, ~reporter,
##2017, 31,
##2017, 854,
##2017, 140,
##2017, 158,
##2017, 188,
##2017, 384,
##2017, 214,
##2017, 818,
##2017, 270,
##2017, 288,
##2017, 320,
##2017, 340,
##2017, 364,
##2017, 417,
##2017, 422,
##2017, 454,
##2017, 462,
##2017, 466,
##2017, 478,
##2017, 496,
##2017, 504,
##2017, 508,
##2017, 104,
##2017, 558,
##2017, 566,
##2017, 585,
##2017, 275,
##2017, 591,
##2017, 646,
##2017, 682,
##2017, 90,
##2017, 662,
##2017, 729,
##2017, 748,
##2017, 804,
##2017, 704,
##2017, 716,
##2016, 31,
##2016, 96,
##2016, 108,
##2016, 120,
##2016, 140,
##2016, 384,
##2016, 320,
##2016, 364,
##2016, 422,
##2016, 454,
##2016, 466,
##2016, 508,
##2016, 104,
##2016, 524,
##2016, 558,
##2016, 800,
##2016, 804,
##2015, 31,
##2015, 44,
##2015, 270,
##2015, 340,
##2015, 296,
##2015, 422,
##2015, 104,
##2015, 800,
##2015, 894,
##2014, 44,
##2014, 364,
##2014, 296,
##2014, 422,
##2014, 496,
##2014, 104,
##2014, 800,
##2014, 894
##)
##
##tps <- mutate(tps, year = as.character(year), reporter = as.character(reporter))
##
##res <- plyr::ddply(
##  tps,
##  .variables = c('year', 'reporter'),
##  .fun = function(x) {
##
##    COMMODITIES <-
##      c("01", "02", "03", "04", "05", "06", "07", "08", "09",
##        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
##        "20", "21", "22", "23", "24",
##        "33", "35", "38",
##        "40", "41", "43",
##        "50", "51", "52", "53"
##      )
##
##    #print(x[["year"]])
##    #stop()
##
##    if (!dir.exists(x[["year"]])) {
##      dir.create(x[["year"]])
##    }
##
##    if (!dir.exists(file.path(x[["year"]], x[["reporter"]]))) {
##      dir.create(file.path(x[["year"]], x[["reporter"]]))
##    }
##
##    d <- data.frame(commodity = COMMODITIES, moved = FALSE, f = NA_character_, from = NA_character_, to = NA_character_, exists = FALSE, stringsAsFactors = FALSE)
##
##    for (i in 1:nrow(d)) {
##      f <- paste0(x[["year"]], "_", x[["reporter"]], "_", d$commodity[i], ".xml")
##      d$f[i] <- f
##      if (file.exists(paste0("C:/Users/mongeau.FAODOMAIN/tmp/trade_data/", x[["year"]], "/", x$reporter, "/", f))) {
##        file.copy(
##          paste0("C:/Users/mongeau.FAODOMAIN/tmp/trade_data/", x[["year"]], "/", x$reporter, "/", f),
##          paste0("C:/Users/mongeau.FAODOMAIN/tmp/trade_data_TP/", x[["year"]], "/", x$reporter, "/", f)
##        )
##        d$moved[i] <- TRUE
##      }
##    }
##
##    return(d)
##  },
##  .progress = "text"
##)
##
##
##
##check_update <- bind_rows(
##plyr::ldply(unique(tldata_2014$rep), .fun = function(x) tibble::tibble(year = 2014, rep = x, rows_down = nrow(tldata_2014[rep == x]), rows_sws = nrow(sws_2014[rep == x]))),
##plyr::ldply(unique(tldata_2015$rep), .fun = function(x) tibble::tibble(year = 2015, rep = x, rows_down = nrow(tldata_2015[rep == x]), rows_sws = nrow(sws_2015[rep == x]))),
##plyr::ldply(unique(tldata_2016$rep), .fun = function(x) tibble::tibble(year = 2016, rep = x, rows_down = nrow(tldata_2016[rep == x]), rows_sws = nrow(sws_2016[rep == x]))),
##plyr::ldply(unique(tldata_2017$rep), .fun = function(x) tibble::tibble(year = 2017, rep = x, rows_down = nrow(tldata_2017[rep == x]), rows_sws = nrow(sws_2017[rep == x])))
##)
##
##
##saveRDS(tldata_2014[rep %in% (check_update %>% filter(rows_down != rows_sws, year == 2014))$rep], 'ct_tariffline_unlogged_2014_ADD.rds')
##saveRDS(tldata_2015[rep %in% (check_update %>% filter(rows_down != rows_sws, year == 2015))$rep], 'ct_tariffline_unlogged_2015_ADD.rds')
##saveRDS(tldata_2016[rep %in% (check_update %>% filter(rows_down != rows_sws, year == 2016))$rep], 'ct_tariffline_unlogged_2016_ADD.rds')
##saveRDS(tldata_2017[rep %in% (check_update %>% filter(rows_down != rows_sws, year == 2017))$rep], 'ct_tariffline_unlogged_2017_ADD.rds')
##
##data.table::fwrite(tldata_2014[rep %in% (check_update %>% filter(rows_down != rows_sws, year == 2014))$rep][, list(tyear, rep, prt, flow, comm, tvalue, weight, qty, qunit, chapter)], 'ct_tariffline_unlogged_2014_ADD.csv')
##data.table::fwrite(tldata_2015[rep %in% (check_update %>% filter(rows_down != rows_sws, year == 2015))$rep][, list(tyear, rep, prt, flow, comm, tvalue, weight, qty, qunit, chapter)], 'ct_tariffline_unlogged_2015_ADD.csv')
##data.table::fwrite(tldata_2016[rep %in% (check_update %>% filter(rows_down != rows_sws, year == 2016))$rep][, list(tyear, rep, prt, flow, comm, tvalue, weight, qty, qunit, chapter)], 'ct_tariffline_unlogged_2016_ADD.csv')
##data.table::fwrite(tldata_2017[rep %in% (check_update %>% filter(rows_down != rows_sws, year == 2017))$rep][, list(tyear, rep, prt, flow, comm, tvalue, weight, qty, qunit, chapter)], 'ct_tariffline_unlogged_2017_ADD.csv')
##
