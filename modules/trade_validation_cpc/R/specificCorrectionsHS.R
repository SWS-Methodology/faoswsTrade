#' Specific corrections to HS data.
#'
#' The function will adjust weight and quantity for given HS codes.
#'
#' NOTE THAT IT IS CURRENTLY SPECIFIC FOR A GIVEN REPORTER AND YEAR.
#'
#' @param tradedata Trade data.
#'
#' @return Data with corrections applied.
#'
#' @import dplyr
#'
#' @export

specificCorrectionsHS <- function(tradedata) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  tradedataname <- tolower(lazyeval::expr_text(tradedata))

  if (tradedataname == 'tldata') {
    
    # XXX This is a very specific correction,
    # should be generalised in a future version
    hs_specific_corrections <- frame_data(
      ~reporter_m49,~year,~correction_flow,~correction_hs,~description,~correction_weight,~correction_qty,~correction_value,~correction_qunit,
      158,2014,NA,"1001","WHEAT",1000,1000,NA,NA,
      158,2014,NA,"1002","RYE",1000,1000,NA,NA,
      158,2014,NA,"1003","BARLEY",1000,1000,NA,NA,
      158,2014,NA,"1004","OATS",1000,1000,NA,NA,
      158,2014,NA,"1005","MAIZE",1000,1000,NA,NA,
      158,2014,NA,"1006","PADDY RICE",1000,1000,NA,NA,
      158,2014,NA,"1006","HUSKED RICE",1000,1000,NA,NA,
      158,2014,NA,"1006","MILLED HUSKED RICE",1000,1000,NA,NA,
      158,2014,NA,"1006","MILLED RICE",1000,1000,NA,NA,
      158,2014,NA,"1006","BROKEN RICE",1000,1000,NA,NA,
      158,2014,NA,"1007","SORGHUM",1000,1000,NA,NA,
      158,2014,NA,"1008","BUCKWHEAT",1000,1000,NA,NA,
      158,2014,NA,"1008","MILLET",1000,1000,NA,NA,
      158,2014,NA,"1008","CANARY SEED",1000,1000,NA,NA,
      158,2014,NA,"1008","FONIO",1000,1000,NA,NA,
      158,2014,NA,"1008","QUINOA",1000,1000,NA,NA,
      158,2014,NA,"1008","TRITICALE",1000,1000,NA,NA,
      158,2014,NA,"1008","MIXED GRAIN",1000,1000,NA,NA,
      158,2014,NA,"1008","CEREALS NES",1000,1000,NA,NA,
      158,2014,NA,"110710","MALT",1000,1000,NA,NA,
      158,2014,NA,"110710","MALT",1000,1000,NA,NA,
      158,2014,NA,"110810","STARCH OF WHEAT",1000,1000,NA,NA,
      158,2014,NA,"110812","STARCH OF MAIZE",1000,1000,NA,NA,
      158,2014,NA,"110813","STARCH OF POTATOES",1000,1000,NA,NA,
      158,2014,NA,"110814","CASSAVA STARCH",1000,1000,NA,NA,
      158,2014,NA,"110819","STARCH OF RICE",1000,1000,NA,NA,
      158,2014,NA,"120110","SOYBEAN",1000,1000,NA,NA,
      158,2014,NA,"120190","SOYBEAN",1000,1000,NA,NA,
      158,2014,NA,"120230","GROUNDNUT IN SHELL",1000,1000,NA,NA,
      158,2014,NA,"120241","GROUNDNUT IN SHELL",1000,1000,NA,NA,
      158,2014,NA,"120242","GROUNDNUT SHELLED",1000,1000,NA,NA,
      158,2014,NA,"120300","COPRA",1000,1000,NA,NA,
      158,2014,NA,"120400","LINSEED",1000,1000,NA,NA,
      158,2014,NA,"120510","RAPESEED",1000,1000,NA,NA,
      158,2014,NA,"120590","RAPESEED",1000,1000,NA,NA,
      158,2014,NA,"120600","SUNFLOWER SEED",1000,1000,NA,NA,
      158,2014,NA,"120710","PALMNUT KERNELS",1000,1000,NA,NA,
      158,2014,NA,"120721","COTTONSEED",1000,1000,NA,NA,
      158,2014,NA,"120729","COTTONSEED",1000,1000,NA,NA,
      158,2014,NA,"120740","SESAME SEED",1000,1000,NA,NA,
      158,2014,NA,"120750","MUSTARD SEED",1000,1000,NA,NA,
      158,2014,NA,"120760","SAFFLOWER SEED",1000,1000,NA,NA,
      158,2014,NA,"120799","OILSEEDS, NES",1000,1000,NA,NA,
      158,2014,NA,"170191","SUGAR REFINED",1000,1000,NA,NA,
      158,2014,NA,"170199","SUGAR REFINED",1000,1000,NA,NA,
      158,2014,NA,"170310","MOLASSES",1000,1000,NA,NA,
      158,2014,NA,"170390","MOLASSES",1000,1000,NA,NA,
      158,2014,NA,"230110","MEAL MEAT",1000,1000,NA,NA,
      158,2014,NA,"230120","MEAL FISH",1000,1000,NA,NA,
      158,2014,NA,"230210","BRAN OF MAIZE",1000,1000,NA,NA,
      158,2014,NA,"230230","BRAN OF WHEAT",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF BARLEY",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF RYE",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF OATS",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF MILLET",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF SORGHUM",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN BUCKWHEAT",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF FONIO",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF TRITICALE",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF MIXED GRAINS",1000,1000,NA,NA,
      158,2014,NA,"230240","BRAN OF CEREALS",1000,1000,NA,NA,
      158,2014,NA,"230250","BRAN OF PULSES",1000,1000,NA,NA,
      158,2014,NA,"230310","GLUTEN FEED&MEAL",1000,1000,NA,NA,
      158,2014,NA,"230320","BEET PULP",1000,1000,NA,NA,
      158,2014,NA,"230320","BAGASSE",1000,1000,NA,NA,
      158,2014,NA,"230330","DREGS FROM BREWING;DIST.",1000,1000,NA,NA
    ) %>%
    distinct(reporter_m49, year, correction_hs, correction_weight, correction_qty)


    if (unique(tradedata$year) %in% unique(hs_specific_corrections$year)) {

      hs_4 <-
        hs_specific_corrections %>%
        dplyr::filter(nchar(correction_hs) == 4) %>%
        dplyr::group_by(correction_hs) %>%
        dplyr::mutate(
          correction_hs_num = as.numeric(correction_hs),
          correction_hs_vec = list((correction_hs_num*100):(correction_hs_num*100+99))
        ) %>%
        dplyr::ungroup() %>%
        tidyr::unnest() %>%
        dplyr::mutate(correction_hs = as.character(correction_hs_vec)) %>%
        dplyr::select(-correction_hs_vec, -correction_hs_num)

      hs_6 <-
        hs_specific_corrections %>%
        dplyr::filter(nchar(correction_hs) == 6)

      hs_specific_corrections <-
        dplyr::bind_rows(hs_4, hs_6) %>%
        dplyr::mutate(correction_hs = as.integer(correction_hs)) %>%
        dplyr::rename(reporter = reporter_m49, hs6 = correction_hs)

      tradedata <-
        tradedata %>%
        dplyr::left_join(hs_specific_corrections, by = c('reporter', 'year', 'hs6')) %>%
        dplyr::mutate(
          weight = ifelse(!is.na(correction_weight), weight*correction_weight, weight),
          qty    = ifelse(!is.na(correction_weight), qty*correction_weight, qty)
        ) %>%
        dplyr::select(-correction_weight, -correction_qty)

    }

  }

  return(tradedata)
  
}

