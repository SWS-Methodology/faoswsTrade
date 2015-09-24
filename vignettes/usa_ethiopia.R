
reporters <- c(231, 238) # The USA and Ethiopia

.ojdbcclasspath <- file.path(Sys.getenv("HOME"), "dstrbs", "ojdbc14.jar")

valid <- fclhs::gettfvalid(reporter = reporters, year = 2011)

tldata <- tldata %>%
  mutate(qunit = as.integer(qunit))

# Units

## Conv. factors
comtradeunits <- read.csv("data-raw/comtrade_units.csv", header = F)
colnames(comtradeunits) <- c("qunit", "wco", "desc", "faoconv")



## units for fcl
data("fclunits",
     package = "tradeproc",
     envir = environment())

tldata <- tldata %>%
  left_join(fclunits,
            by = "fcl") %>%
  left_join(comtradeunits,
            by = "qunit")


## Check of correspondence between unites

tldata %>%
  select(fcl, fclunit, ctunit = wco, ctunitdesc = desc, faoconv) %>%
  distinct() %>%
  filter(fclunit != "mt" & !(ctunit %in% c("kg", "l"))) %>%
  as.data.frame

### TODO add warning if kg in heads etc.


## Creating table for conversion

tldata %>%
  filter(reporter %in% reporters) %>%
  select(ctunitcode = qunit, fclunit, ctunit = wco, ctunitdesc = desc) %>%
  distinct() %>%
  filter(!is.na(fclunit)) %>%
  arrange(ctunitcode) %>%
  write.table(file = "data-raw/ctfclunitconv.csv", sep = ",",
              row.names = F)

#### NA in fclunit????

tldata %>%
  select(fcl, fclunit, ctunit = wco, ctunitdesc = desc, faoconv) %>%
  distinct() %>%
  filter(is.na(fclunit) & !is.na(fcl)) %>%
  select(fcl) %>%
  distinct() %>%
  arrange(fcl)


### Problematic unit correspondings

read.table(file = "data-raw/ctfclunitconv.csv",
           sep = ",",
           header = T,
           stringsAsFactors = F) %>%
  filter(fclunit != "$ value only" & is.na(ctfclconvfactor) & ctunit != "-") %>%
  inner_join(tldata %>%
               select(year, reporter, partner, flow, fcl, qunit, fclunit, qty, value) %>%
               filter(reporter %in% reporters),
             by = c("ctunitcode" = "qunit",
                    "fclunit")) %>%
  mutate(fcldesc = fclhs::descfcl(fcl)) %>%
  select(year, reporter, partner, flow, fcl, fcldesc, qty, value, fclunit, ctunitdesc) %>%
  arrange(year, reporter, flow, fcl, fclunit) %>%
  XLConnect::writeWorksheetToFile(data = .,
                                  file = "data-raw/datatocheckunits.xlsx",
                                  sheet = "datatocheckunits")
  write.table(file = "data-raw/datatocheckunits.csv", sep = ",",
              row.names = F)



# Alex, could you kindly give me examples of which commodities and which countries.
# Thanks.
# Salar
tldata %>%
  mutate(qunit = as.integer(qunit)) %>%
  group_by(qunit) %>%
  summarize(tradeflows = n()) %>%
  left_join(units,
            by = c("qunit" = "code")) %>%
  arrange(desc(tradeflows))

data("FAOcountryProfile",
     package = "FAOSTAT",
     envir = environment())

faoareanames <- FAOcountryProfile %>%
  select(fao = FAOST_CODE,
         faoname = SHORT_NAME)

tldata %>%
  select(reporter, partner, fcl, qunit) %>%
  mutate(qunit = as.integer(qunit)) %>%
  filter(qunit != 8) %>%
  group_by(qunit) %>%
  mutate(tradeflows = n()) %>%
  sample_n(3) %>%
  ungroup() %>%
  left_join(units,
            by = c("qunit" = "code")) %>%
  arrange(desc(tradeflows)) %>%
  left_join(faoareanames,
            by = c("reporter" = "fao")) %>%
  select(-reporter) %>%
  rename(reporter = faoname) %>%
  left_join(faoareanames,
            by = c("partner" = "fao")) %>%
  select(-partner) %>%
  rename(partner = faoname) %>%
  mutate(fcl = fclhs::descfcl(fcl)) %>%
  select(qunit, wco, desc, fcl, reporter, partner, tradeflows) %>%
  arrange(desc(tradeflows)) %>%
  as.data.frame
