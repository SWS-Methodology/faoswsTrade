
# Get leaves of a code tree
GetLeaves <- function (tree) {
    
    # Children of ANY root in the tree (superset of the leaves)
    children <- faoswsUtil::adjacent2edge(tree)$children
    
    # Roots in the tree
    roots <- tree$parent
    
    # Exclude roots from children to get leaves
    leaves <- children[!(children %in% roots)]
    
}


# Get domain from dataset
get_domain <- function (dataset) {
    
    # All domains
    all_domains <- GetDomainNames()
    
    # All datasets for each domain
    all_datasets <- sapply(all_domains, GetDatasetNames, USE.NAMES = TRUE)
    
    # Domain where the dataset is
    domain <- names(Filter(f = function (dom) dataset %in% dom, 
                           x = all_datasets))
    #dom_pos <- Position(function (dom) 'aproduction' %in% dom, all_datasets)
    #names(all_datasets)[dom_pos]
    
    # Throw error if no domain is found
    if (isTRUE(length(domain) == 0)) {
      stop(message(paste0("The domain for dataset '", dataset, "' cannot be retrieved. The user may not have the rights to see the dataset.")))
    }
    
    # Return domain
    return(domain)
    
}


# Get dimensions from dataset
get_dataset_dim <- function(dataset,
                            domain = NULL) {
    
    # Retrieve domain from dataset
    if (is.null(domain)) domain <- get_domain(dataset)
    
    # Retrieve dimensions
    dataset_dim <- GetDatasetConfig(domain, dataset)$dimensions
    
    # Return dimensions
    return(dataset_dim)
    
}


# Get the keys queried by the user
query_keys <- function(dim_name) {
  
  # Retrieve keys
  keys <- swsContext.datasets[[1]]@dimensions[[dim_name]]@keys
  
  # Return keys
  return(keys)
  
}
  

# Import data from SWS
get_sws_data <- function(domain = NULL,
                         dataset,
                         keys = NULL,
                         flag = F) {
    
    # Retrieve domain from dataset
    if (is.null(domain)) domain <- get_domain(dataset)
    
    # Dimensions' names
    dim_names <- GetDatasetConfig(domain, dataset)$dimensions
    ndim <- length(dim_names)
    
    # TODO: provide option to select all the keys for a dimension?
    # dim_keys <- GetCodeList(domain, dataset, dimension)[, code])
    
    # Check that the value of key is a list
    stopifnot("The value of 'keys' must be a list." = (is.list(keys)))
    
    # Check that the number of dimensions is correct
    if (isFALSE(length(keys) == ndim)) 
        writeLines(c("Please, provide keys in a list with the following order:",
                     paste(1:ndim, dim_names, sep = ". ")))
    stopifnot('Wrong number of dimensions.' = (length(keys) == ndim))
    #also includes the case where keys is NULL
    
    # Turn keys into dimensions
    dimensions <- mapply(FUN = function(dim_name,
                                        dim_keys) Dimension(name = dim_name,
                                                            keys = dim_keys),
                         dim_names,
                         keys,
                         SIMPLIFY = FALSE)
    
    # Obtain dataset key
    data_key <- DatasetKey(domain = domain,
                           dataset =  dataset,
                           dimensions = dimensions)
    
    # Get data, accounting for flags
    if (isTRUE(flag)) data <- GetData(data_key)
    else data <- GetData(data_key, flags = F)
    
    # Return dataset
    return(data)
    
}


# Function to import data from SWS and turn it into a dataset
sws2dataset <- function (domain = NULL,
                         dataset,
                         keys = NULL,
                         flag = F,
                         elem_colnames = NULL) {
    
    # Retrieve domain from dataset
    if (is.null(domain)) domain <- get_domain(dataset)
    
    ## Import data from SWS ##
    sws_data <- get_sws_data(domain = domain,
                             dataset = dataset,
                             keys = keys,
                             flag = flag)
    
    ## Transform data.table from long to wide, with a column for each element ##
    
    # Column names
    cnames <- colnames(sws_data)
    
    # Element dimension's names
    elem <- grep(pattern = 'element',
                 x = cnames,
                 ignore.case = TRUE,
                 value = TRUE)
    
    # Other dimensions' names
    other <- grep(pattern = 'element|value',
                  x = cnames,
                  ignore.case = TRUE,
                  value = TRUE,
                  invert = TRUE)
    
    # Formula string
    form_char <- paste(paste(other,
                             collapse = ' + '),
                       '~', elem)
    # In the form of dim1 + dim2 + ... + dimJ ~ element (suppose that element is dim0)
    
    # Convert data into a wide dataset
    wide_data <- dcast(data = sws_data,
                       formula = as.formula(form_char),
                       value.var = 'Value')
    
    ## Change column names for the elements ##
    
    # Retrieve the elements
    elem_codes <- unique(unlist(sws_data[, elem, with = FALSE]))
    # stopifnot('Wrong input.' = c(length(elem_colnames) == length(elem_codes), is.null(elem_colnames)))
    
    # Check if element column names are provided
    if (!is.null(elem_colnames)) {
        
        # Check if the number of element column names is correct
        stopifnot("The length of elem_colnames should be equal to the number of elements."
                  = length(elem_colnames) == length(elem_codes))
        
        # Use the names to rename the columns
        colnames(wide_data)[colnames(wide_data) %in% elem_codes] <- elem_colnames
        # TODO: add in the documentation that elem_colnames should be provided
        #       in the same order as the element key (or think of a check?)
        
    } else {
        
        # If the names are not provided, throw warning
        warning("No elem_colnames given, the element columns names will be set automatically.",
                call. = FALSE)
        
        # Create automatic names
        auto_elem_colnames <- sapply(elem_codes, function (ecode) {
            
            # Retrieve the description of each key
            descr <- GetCodeList(domain = domain,
                                 dataset = dataset,
                                 dimension = elem,
                                 codes = ecode)$description
            
            # Strip out parentheses
            clean_descr <- unlist(strsplit(descr,
                                           split = ' [',
                                           fixed = TRUE))[1]
            
            # Camel description
            camel_descr <- gsub(pattern = ' ',
                                replacement = '',
                                x = clean_descr)
            
            # Change first letter to lowercase
            substr(camel_descr, 1, 1) <- tolower(substr(camel_descr, 1, 1))
            
            # Return column name
            return(camel_descr)
            
        })
        
        # Use the automatic names to rename the columns
        colnames(wide_data)[colnames(wide_data) %in% elem_codes] <- auto_elem_colnames
        
    }
    
    # Return dataset
    return(wide_data)
    
}


# Check when countries are reporter countries
# NB.: built starting from countryExists; see its documentation for further info
countryReports <- function (countries,
                            years,
                            #use_country_names = FALSE,
                            warnNA = TRUE) {
  
  # Warning in case of NAs
  if(any(is.na(countries) | is.na(years)) & warnNA){
    warning("NA in countries or years - if you're subsetting, this will probably return nothing.")
  }
  
  # Check that countries and years have the same length
  stopifnot(length(countries) == length(years))
  
  # Check that countries and years are all character
  stopifnot('Values for input parameters countries and years should be character.' = c(is.character(years), is.character(countries)))
  
  # Use description or code column for countries based on use_country_names
  #country_colname <- ifelse(use_country_names, "description", "code")
  
  # Reporters by year datatable
  reporters <- faosws::ReadDatatable(table = "reporters_by_year_new_version")
  
  # Change column names for years columns
  colnames(reporters) <- sub('year_', '', colnames(reporters))
  
  # Exclude years for which there are no reporter countries
  na_number <- colSums(is.na(reporters))
  nonempty_cols <- colnames(reporters)[na_number < nrow(reporters)]
  reporters <- reporters[, ..nonempty_cols]
  
  # Combine rows with the same m49 code
  reporters <- suppressWarnings(setDT(reporters)[, 
                                                 lapply(.SD, function (x) unique(na.omit(x))), 
                                                 by = m49])
  # Necessary because there are multiple rows with the same m49 codes but with
  # different value in the 'code' column.
  # Now keep one row for each m49 code (multiple ones were kept because 'code' is different)
  reporters <- unique(reporters, by = 'm49')
  
  # List where each element is the vector of years for which the corresponding 
  # country is reporting
  report_list <- apply(X = reporters, 
                       MARGIN = 1,
                       FUN = function (i) colnames(reporters)[i %in% 'X'])
                       # %in% in place of == so that NAs are discarded
  
  # Create new data.table with country codes and vectors of years 
  report_dt <- data.table(code = reporters$m49,
                          reports = report_list)
  
  # Create data.table from the provided countries and years
  call_dt <- data.table(country = countries,
                        year = years)
  
  # Merge the two data.tables
  merged_dt <- merge(x = call_dt,
                     y = report_dt, 
                     by.x = 'country',
                     by.y = 'code',
                     sort = FALSE)
  # NB.: only countries contained in both datatables are kept
  
  # Is the country a reporter country in a given year?
  is.reporter <- mapply(FUN = function (year, reports) year %in% reports,
                        merged_dt$year,
                        merged_dt$reports,
                        USE.NAMES = FALSE)

  # Return logical vector
  return(is.reporter)
  
}

