library('data.table')

# ESS chapters
COMMODITIES <-
  c("01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24",
    "33", "35", "38",
    "40", "41", "43",
    "50", "51", "52", "53")


f <- 'c:/Users/mongeau.FAODOMAIN/tmp/trade_data_USA_2017/Tariffline-USA-2017-newformat.txt'

d <- fread(f, skip = 1, header = TRUE, colClasses = "character")

# Imports CIF, exports FOB
d[, tvalue := ifelse(cifValueOrig != "", cifValueOrig, fobValueOrig)]

d <-
  d[,
    .(
      chapter       = substr(cmdCodeOrig, 1, 2),
      rep           = reporterCode,
      tyear         = substr(refPeriodId, 1, 4),
      curr          = NA_character_,
      hsrep         = classificationCode,
      flow          = flowCode, # codes: M, DX, RX
      repcurr       = NA_character_,
      comm          = cmdCodeOrig,
      prt           = partnerCode,
      weight        = as.numeric(netWgt),
      qty           = as.numeric(qty),
      qunit         = qtyUnitCode,
      qtyUnitDesc,
      tvalue        = as.numeric(tvalue),
      est           = NA_character_, # in tldata is always 0
      ht            = NA_character_  # in tldata is always 4
    )
  ]

d[
  qtyUnitDesc == "Weight in thousand of kilograms",
  `:=`(
    qty = qty * 1000,
    qtyUnitDesc = "Weight in kilograms",
    qunit = "8"
  )
]

d[
  qtyUnitDesc == "Weight in grams",
  `:=`(
    qty = qty / 1000,
    qtyUnitDesc = "Weight in kilograms",
    qunit = "8"
  )
]

d[qunit == "-1", qunit := "1"]

d[, .N, keyby = .(as.numeric(qunit), qtyUnitDesc)]

d[chapter %chin% COMMODITIES][, .N, keyby = .(as.numeric(qunit), qtyUnitDesc)]

# Take only these as for other commodities there are other qunits that are not
# codified in http://unstats.un.org/unsd/tradekb/Attachment67.aspx?AttachmentType=1
# (main page: https://unstats.un.org/unsd/tradekb/Knowledgebase/50146/Quantity-and-Weight-Data-in-UN-Comtrade )

d_ess <- d[chapter %chin% COMMODITIES]

d_ess <- d_ess[, -grep("qtyUnitDesc", names(d_ess)), with = FALSE]

d_ess[flow == "M", flow := "1"]
d_ess[flow == "DX", flow := "2"]
d_ess[flow == "RX", flow := "3"]

options(scipen=999)

write.csv(d_ess, 'c:/Users/mongeau.FAODOMAIN/tmp/trade_data_USA_2017/d_ess.csv', row.names = FALSE)

# NOTE: The file needs to be imported to SWS by skipping the first line (header)

