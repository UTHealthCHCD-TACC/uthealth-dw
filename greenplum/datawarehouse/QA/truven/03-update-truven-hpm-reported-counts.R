# install and load necessary packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(RPostgres, DBI, keyring, stringr, tidyverse)


# Create a function called getNumObservations() to pull the number of observations from a .txt file and add it (along with the corresponding
# year and table name) to a dataframe (df)

getNumObservations <- function(year){
  
  # Creates an empty df to store year, table name, number of observations, and report version for each file that is read
  numObservationsDF <- data.frame(Year = character(), TableName = character(), ReportedNumObservations = integer(), ReportVersion = character())
  
  # Sets working directory for a given year
  setwd(paste0('V:/Data Dictionary/Data Quality Reports/HPM/',year))
  
  # Retrieves and stores the HPM file names from a directory for a given year
  file_names <- list.files(paste0('V:/Data Dictionary/Data Quality Reports/HPM/',year))
  
  # Loops through file_names to pull the number of observations and report version, and add the values to numObservationsDF (along with corresponding table 
  # name and year) for each HPM .txt file in the working directory
  
  for (i in 1:length(file_names)){
    
    # Reads text file
    readTxt <- readLines(file_names[i], warn = FALSE)
    
    # Finds and retrieves line of the first occurrence of 'Number of Observations in Dataset=' within the .txt file
    numObservationsTxtLine <- readTxt[grepl('Number of Observations in Dataset=', readTxt)][1]
    
    # Formats and cleans up the line read from the .txt file (numObservationsTxtLine):
    # sub() is used to select everything after the '=' in the string
    # str_replace_all() is used to remove dashes, empty space, and commas from the string
    
    numObservations <- str_replace_all(sub('.+=(.+)', '\\1', numObservationsTxtLine), c('-' = '', ' ' = '', ',' = ''))
    
    # Since the report version is located in different locations within the files, 
    # conditional statements are used to pull the report version from the .txt files. 
    
    # This line will find line number y with the occurrence of 'version', and then select x lines below it
    # reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt) + x][y]
    
    # This line will extract the version number from the string and removes any dashes, empty space, or commas that may be in the string
    # reportVersion <- str_replace_all(substring(reportVersionTxtLine, a, b), c('-' = '', ' ' = '', ',' = ''))
    
    if (year %in% c(2015, 2016, 2017) & substring(file_names[i],1,2) %in% c('lt', 'st', 'wc')){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][5]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 1,40), c('-' = '', ' ' = '', ',' = ''))
      
    }else if (year < 2018){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][3]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 1,40), c('-' = '', ' ' = '', ',' = ''))
      
    }else if(year == 2018 & substring(file_names[i],1,2) == 'el'){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][2]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 125,128), c('-' = '', ' ' = '', ',' = ''))
      
    }else if(year == 2018 & substring(file_names[i],1,2) == 'ab'){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][2]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 114,120), c('-' = '', ' ' = '', ',' = ''))
      
    }else if(year == 2018 & substring(file_names[i],1,2) %in% c('lt', 'st')){
      
      reportVersionTxtLine <- readTxt[grep('Term Disability', readTxt)+14][4]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 122,126), c('-' = '', ' ' = '', ',' = ''))
      
    }else if(year %in% c(2018, 2019) & substring(file_names[i],1,2) == 'wc'){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][3]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 98,105), c('-' = '', ' ' = '', ',' = ''))
      
    }else if(year == 2019 & substring(file_names[i],1,2) %in% c('ab', 'el')){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][2]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 110,118), c('-' = '', ' ' = '', ',' = ''))
      
    }else if(year == 2019 & substring(file_names[i],1,2) %in% c('lt', 'st')){
      
      reportVersionTxtLine <- readTxt[grep('Term Disability', readTxt)+14][3]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 120,123), c('-' = '', ' ' = '', ',' = ''))
      
    }else if (year %in% c(2020, 2021) & substring(file_names[i],1,2) %in% c('lt', 'st', 'wc')){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][5]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 1,40), c('-' = '', ' ' = '', ',' = ''))
      
    }else if (year %in% c(2020, 2021) & substring(file_names[i],1,2) == 'ab'){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][2]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 110,115), c('-' = '', ' ' = '', ',' = ''))
      
    }else if (year %in% c(2020, 2021) & substring(file_names[i],1,2) == 'el'){
      
      reportVersionTxtLine <- readTxt[grep('Version|VERSION|version', readTxt)+2][2]
      reportVersion <- str_replace_all(substring(reportVersionTxtLine, 115,120), c('-' = '', ' ' = '', ',' = ''))
      
    }
    
    # Adds the number of observations and report version, along with the corresponding table name and corresponding year, as a row to df: numObservationsDF
    numObservationsDF[nrow(numObservationsDF) +1,] <- c(year, paste0('hpm_', sub('2.*', '', file_names[i])), numObservations, reportVersion)
    
  }
  
  # Returns populated df for a given year
  return(numObservationsDF)
}


# Range of years to retrieve number of observations for is defined
definedYears <- 2011:2021

# Create object to store and append dfs to
finalNumObservationsDF <- vector()

# Uses for loop to call function for each year in definedYears. Adds returned df to a final df: finalNumObservationsDF
for(k in 1:length(definedYears)){
  
  finalNumObservationsDF <- rbind(finalNumObservationsDF, getNumObservations(definedYears[k]))
  
}

# Show final df
#finalNumObservationsDF


# Connect to Greenplum
tac <- dbConnect(RPostgres::Postgres(),
                 dbname = "uthealth",
                 user = "sharrah17",
                 password = key_get("Greenplum", "sharrah17"),
                 host = "greenplum01.corral.tacc.utexas.edu")


# Write table to csv before updating truven_counts table
# write.csv(dbGetQuery(tac,"select * from qa_reporting.truven_counts;"), "H:/Truven_Cnts_Before_Update.csv")

# Backup table before updating
# dbExecute(tac,"truncate table qa_reporting.truven_counts_old")

# dbExecute(tac,"insert into qa_reporting.truven_counts_old select * from qa_reporting.truven_counts")


# Write Queries to update truven_counts table

# Pull and store rows from truven_counts table where reported_row_count is NULL or row_count_difference is not zero
rowsToBeUpdated <- dbGetQuery(tac,"select * from qa_reporting.truven_counts where reported_row_count is null or row_count_difference  != 0 and table_name like 'hpm%';")

# Loop through rows in rowsToBeUpdated
for(j in 1:length(rowsToBeUpdated$row_count)){
  
  # Get ReportedNumObservations from finalNumObservationsDF for a given year and table name
  reportedNumObservation <- paste(finalNumObservationsDF %>% filter(Year == rowsToBeUpdated$year[j] & TableName == rowsToBeUpdated$table_name[j]) %>%  select (ReportedNumObservations))
  
  # Get ReportedVersion from finalNumObservationsDF for a given year and table name
  reportedVersion <- paste(finalNumObservationsDF %>% filter(Year == rowsToBeUpdated$year[j] & TableName == rowsToBeUpdated$table_name[j]) %>%  select (ReportVersion))
  
  # Update statement to populate the reported_row_count and report_version cols
  dbExecute(tac,paste0("update qa_reporting.truven_counts set reported_row_count = ", reportedNumObservation, 
                       ", report_version = ", reportedVersion, " where year = '", rowsToBeUpdated$year[j],"' and table_name = '",rowsToBeUpdated$table_name[j],"';"))
  
  # Update statement to calculate row_count_difference and add a date to col last_updated
  dbExecute(tac,paste0("update qa_reporting.truven_counts set row_count_difference = row_count - reported_row_count, last_updated = CURRENT_DATE where year = '", rowsToBeUpdated$year[j],"' and table_name = '",rowsToBeUpdated$table_name[j],"';"))
  
  # Update statement to calculate row_percent_difference
  dbExecute(tac,paste0("update qa_reporting.truven_counts set row_percent_difference = 100. * abs(row_count - reported_row_count)/reported_row_count where year = '", rowsToBeUpdated$year[j],"' and table_name = '",rowsToBeUpdated$table_name[j],"';"))
  
}


