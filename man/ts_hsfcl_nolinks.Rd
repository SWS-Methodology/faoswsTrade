% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ts_hsfcl_nolinks.R
\name{ts_hsfcl_nolinks}
\alias{ts_hsfcl_nolinks}
\title{Extract all unmapped HS codes across years}
\usage{
ts_hsfcl_nolinks(collection_path = NULL, prefix = NULL)
}
\arguments{
\item{collection_path}{String with path where single-year report directories
are stored.}

\item{prefix}{String with report directory name prefix without trailing
underscore. By default NULL.}
}
\description{
Extract all unmapped HS codes across years
}
\examples{
\dontrun{
library(ggplot2)
library(dplyr)

uniq_hs <- ts_hsfcl_nolinks("/mnt/storage/sws_share/sas",
                            "complete_tf_cpc")
uniq_hs \%>\%
  group_by(year, flow) \%>\%
  dplyr::summarize(uniq_hs = n()) \%>\%
  ggplot(aes(as.factor(year), uniq_hs)) +
  geom_bar(stat = "identity") +
  ylab("Number of unique unmapped HS") + xlab("") +
  ggtitle(paste0("There are ",
                 nrow(uniq_hs),
                 " unmapped HS codes"),
          "New reporters absent in the map are not included")
}

}
