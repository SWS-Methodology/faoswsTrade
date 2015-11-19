#' Returns subset of HS-codes of agriculture items used by ESS devision.
#' 
#' List of agricultural commodities provided by Cladia DeVita.
#'@return Character vector 
getAgriHSCodes <- function() {
  
  # Eventually table should be moved in ad hoc SWS table
  
  agriItems <- read.table(header = T, text = "
                          fromCode	toCode
                          10100	10699
                          20100	20299
                          20300	21099
                          30760	30899
                          40100	41099
                          50100	51199
                          60100	60499
                          70100	71499
                          80100	81499
                          90100	91099
                          100100	100899
                          110100	110799
                          110811	110819
                          110900	110999
                          120100	121499
                          130100	130299
                          140100	140499
                          150100	151899
                          152100	152299
                          160100	160399
                          160558	160558
                          170100	170499
                          180100	180699
                          190100	190599
                          200100	200999
                          210100	210699
                          220100	220999
                          230100	230999
                          240100	240399
                          330100	330199
                          350110	350190
                          350211	350219
                          382310	382319
                          400100	400199
                          410100	410399
                          411520	411520
                          430100	430199
                          500100	500399
                          510100	510599
                          520100	520399
                          530100	530599
                          530810	530810
                          ")
    
  # Converts boundaries to vector of all possible integers
  agriItems <- unname(unlist(apply(agriItems, 1, function(x) seq.int(x[1], x[2], by = 1))))
  
  # Leading zero for short codes
  agriItems <- sprintf("%06d", agriItems)
  
  allItems <- getAllItems()$code
  
  allItems[allItems %in% agriItems]
}
