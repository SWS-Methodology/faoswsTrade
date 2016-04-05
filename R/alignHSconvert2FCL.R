#' Align HS codes in trade data set and in HS2FCL map and converts them to FCL
#'
#' @import dplyr
#' @export

alignHSconvert2FCL <- function(tldata, hsfclmap, parallel) {

  ###  Calculate length of hs codes in TL

  tldata <- tldata %>%
    group_by_(~reporter, ~flow) %>%
    mutate_(tlmaxlength = ~max(stringr::str_length(hs), na.rm = TRUE)) %>%
    ungroup()


  ## Dataset with max length in TL ####


  tlmaxlength <- tldata %>%
    select_(~reporter,
           ~flow,
           ~tlmaxlength) %>%
    group_by_(~reporter, ~flow) %>%
    summarize_(tlmaxlength = ~max(tlmaxlength, na.rm = TRUE)) %>%
    ungroup()


  ## Common max length between TL and map ####


  maxlengthdf <- tlmaxlength %>%
    left_join(mapmaxlength,
              by = c("reporter" = "area", "flow")) %>%
    group_by_(~reporter, ~flow) %>%
    mutate_(maxlength = ~max(tlmaxlength, mapmaxlength, na.rm = TRUE)) %>%
    # na.rm here: some reporters are absent in map
    #  122 145 180 224 276
    ungroup()

  ### Extension of HS-codes in TL ####

  tldata <- tldata %>%
    select_(~-tlmaxlength) %>%
    left_join(maxlengthdf %>%
                select_(~-tlmaxlength, ~-mapmaxlength),
              by = c("reporter", "flow")) %>%
    mutate_(hsext = ~as.numeric(hsfclmap::trailingDigits2(hs,
                                                          maxlength = maxlength,
                                                          digit = 0)))

  ### Extension of HS ranges in map ####


  hsfclmap1 <- hsfclmap %>%
    left_join(maxlengthdf %>%
                select_(~-tlmaxlength, ~-mapmaxlength),
              by = c("area" = "reporter", "flow")) %>%
    filter_(~!is.na(maxlength))                                         ## Attention!!!

  hsfclmap1 <- hsfclmap1 %>%
    mutate_(fromcode = ~as.numeric(hsfclmap::trailingDigits2(fromcode, maxlength, 0)),
            tocode = ~as.numeric(hsfclmap::trailingDigits2(tocode, maxlength, 9)))


  ########### Mapping HS codes to FCL in TL ###############

  convertHS2FCL(tldata, hsfclmap1, parallel = TRUE)
}
