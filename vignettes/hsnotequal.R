data("FAOcountryProfile", package = "FAOSTAT")
faoareanames <- FAOcountryProfile %>%
  select(reporter = FAOST_CODE,
         name = SHORT_NAME)

d <- plyr::alply(maxlengthdf %>%
              filter(tlmaxlength != mapmaxlength) %>%
              select(reporter, flow),
            .margins = 1,
            function(x) {
              l <- list(map = hsfclmap %>%
                          select_(reporter = ~area, ~flow, ~fromcode, ~tocode, ~fcl) %>%
                          filter_(~reporter == x$reporter & flow == x$flow) %>%
                          select_(~fromcode, ~tocode, ~fcl) %>%
                          sample_n(3) %>%
                          left_join(faoareanames) %>%
                          as.data.frame(),
                        data = tldata %>%
                          select_(~reporter, ~flow, ~hs) %>%
                          filter_(~reporter == x$reporter & flow == x$flow) %>%
                          select_(~hs) %>%
                          sample_n(5) %>% as.data.frame
              )
              names(l) <- c(paste0(x$reporter, " - ", x$flow, ". Example of map: "),
                            "Example of data: ")
              l
            },
            .expand = T)


maxlengthdf %>%
  filter(tlmaxlength != mapmaxlength) %>%
  left_join(faoareanames) %>%
  select(faoarea = reporter, area = name, flow, tlmaxlength, mapmaxlength) %>%
  as.data.frame
