### Loading required packages.
library(rJava)
library(RJDBC)
library(sqldf)
library(plyr)
library(foreach)
library(doParallel)
library(data.table)
library(pipeR)
library(stringr)

options(stringsAsFactors = FALSE)
setwd('/home/CL_US_Crude_Prod')

## Load all the user-defined functions.
  # The source file should be put in the working directory.
source("/home/CL_US_Crude_Prod/us_crude_prod/function_source.R")
source("/home/CL_US_Crude_Prod/us_crude_prod/getDBInfo.R")

## Set the driver of the PostgreSQL, the driver file (postgresql-9.4.1207.jar) must be downloaded first.
pgsql <- JDBC("org.postgresql.Driver", "/home/CL_US_Crude_Prod/postgresql-9.4.1207.jar", "`")

## Establish the connection with Database Prod!

Prod_DB <- getDBInfo('read_write')
prod_base <- dbConnect(pgsql, url = Prod_DB$PROD_URL,
                user = Prod_DB$PROD_USERNAME,
                password = Prod_DB$PROD_PASSWORD)

# Dev_DB <- getDBInfo('dev')
# dev_base <- dbConnect(pgsql, url = Dev_DB$DEV_URL,
#                       user = Dev_DB$DEV_USERNAME,
#                       password = Dev_DB$DEV_PASSWORD)
#--------------------------------------------------------------------------------------------------#
#--------------------------------------------------------------------------------------------------#
start_time = proc.time()

#----------------------------------------#
#              Initializing              #
#----------------------------------------#

## For table extraction
BASIN_SET <- "('PERMIAN BASIN')"
BASIN <- 'PERMIAN BASIN'
PROD_TBL <- 'cpm.crd_prod_base'
DCL_TBL <- 'cpm.crd_prod_adj_dcl_basin'
BASIN_MAX_MTH_TBL <- 'cpm.crd_prod_basin_max_mth'

## Multi threads setting.
numberOfWorkers = 16

# Init Info. @10th Function
initializing(BASIN_SET, PROD_TBL, DCL_TBL, BASIN_MAX_MTH_TBL, numberOfWorkers)
## ------------------------------------ ##

## ------------------ ##
##    Data Loading    ##
## ------------------ ##

## Loading drilling info data set here.
PRODUCTION <- partition_load(prod_base, BASIN_SET, part_num = 10, tbl_name = PROD_TBL)
# PRODUCTION <- dbGetQuery(prod_base, sprintf("select * from %s
#             where basin in %s", PROD_TBL, BASIN_SET))
#------------#
cat('Production data was loaded successfully...\n')
#------------#

## Change data structure into data.table
PRODUCTION <- as.data.table(PRODUCTION)

## Set keys for faster searching.
setkey(PRODUCTION, entity_id, basin, first_prod_year, n_mth)

PRODUCTION[, comment := ""]
PRODUCTION[, last_prod_date := as.character(last_prod_date)]
PRODUCTION[, prod_date:= as.character(prod_date)]

## choose the max date of available data

cutoff_date <- paste0("select (max(date))::DATE cutoff_date from ei.ei_flat where seriesid = 'PET.MCRFPUS2.M'") %>>%
{dbGetQuery(prod_base, .)}

cutoff_date <- cutoff_date$cutoff_date
# cutoff_date <- as.Date(max(PRODUCTION[,prod_date]))

## Load decline rate data for the basin.
dcl <- dbGetQuery(prod_base, sprintf("select * from %s where basin in %s", DCL_TBL, BASIN_SET))
dcl <- as.data.table(dcl)
setkey(dcl, basin, first_prod_year, n_mth)

#------------#
cat('Decline rate data was loaded successfully...\n')
#------------#

### Load basin maximum production month table.
basin_max_mth_tbl <- dbGetQuery(prod_base, sprintf("select * from %s", BASIN_MAX_MTH_TBL))
basin_max_mth_tbl <- as.data.table(basin_max_mth_tbl)
setkey(basin_max_mth_tbl, basin, first_prod_year)


#-----------------------------------------------------------------#
# Part 1 -- Filling entity production which is actually not zero. #
#-----------------------------------------------------------------#
## In this part, two functions need to be loaded from function_source:
## @7th: filling_zero(), and @8th update_table()

### Find entity with zero production.
zero <- sqldf("select entity_id
              from PRODUCTION
              where last_prod_date = prod_date and liq = 0")

zero <- as.data.table(zero)
setkey(zero, entity_id)

### Main part.
cl_zero <- makeCluster(numberOfWorkers)
registerDoParallel(cl_zero)

#------------#
cat(sprintf('The number of threads is %i...\n', numberOfWorkers))
cat('#-------------------------------------------#\n')
cat(sprintf('Start filling zero at %s...\n', as.character(Sys.time())))
#------------#

tic_zero = proc.time() # Record the start time...
# collect all the updated entries.
fill_zero = foreach(i = 1:nrow(zero), .combine = rbind, .packages =  'data.table') %dopar%
  filling_zero_v_2(i, prod_tbl = PRODUCTION)
stopCluster(cl_zero)

# update the original table with values calculated in the last step.
PRODUCTION <- update_table(orig_tbl = PRODUCTION, update_val_tbl = fill_zero)
setkey(PRODUCTION, entity_id, basin, first_prod_year, n_mth)
toc_zero = proc.time() # Record ending time.
time_usage_zero = toc_zero - tic_zero
time_usage_zero



#------------#
cat(sprintf('Filling zero was executed successfully at %s...\n', as.character(Sys.time())))
cat('#-------------------------------------------#\n')
#------------#


#-----------------------------------------#
# Part Two -- Filling the missing values. #
#-----------------------------------------#

#------------#
cat(sprintf('Start filling missing values at %s...\n',as.character(Sys.time())))
#------------#


## Entity with missing data
missing <- sqldf("with t0 as (
                 select entity_id, avg(liq) as avg
                 from PRODUCTION
                 group by entity_id)

                 select *
                 from PRODUCTION
                 where entity_id in (select entity_id from t0 where avg >= 15)
                 and prod_date = last_prod_date")

missing <- subset(missing, last_prod_date < cutoff_date)
missing <- as.data.table(missing)
setkey(missing, entity_id, basin, first_prod_year, n_mth)
missing[, last_prod_date := as.character(last_prod_date)]
missing[, prod_date := as.character(prod_date)]

## Change data type for table union.
PRODUCTION[, last_prod_date := as.character(last_prod_date)]
PRODUCTION[, prod_date := as.character(prod_date)]

## Main program.
tic_missing = proc.time()
    if (nrow(missing) != 0){
      cl_miss <- makeCluster(numberOfWorkers)
    registerDoParallel(cl_miss)

    fill_miss = foreach(i = 1:nrow(missing), .combine = rbind, .packages = 'data.table') %dopar%
      filling_missing(i, cutoff_date)
    stopCluster(cl_miss)

    setkey(fill_miss, entity_id, n_mth)
    # Update last_prod_date for entities who are filled.
    last_prod_date_tbl <- fill_miss[!duplicated(entity_id, fromLast = T), .(entity_id, last_prod_date)]
    updated_date = last_prod_date_tbl[1, last_prod_date]
    # Add fill_miss table into original production table.
    PRODUCTION <- rbindlist(list(PRODUCTION, fill_miss))
    # Due to the cutoff date is set, so the last_prod_dates for filled data are the same.
    PRODUCTION[entity_id %in% last_prod_date_tbl[, entity_id], last_prod_date := updated_date]
    setkey(PRODUCTION, entity_id, basin, first_prod_year, n_mth)
} else{
  cat("There is no missing values...\n")
}

toc_missing = proc.time()
time_usage_missing = toc_missing - tic_missing
time_usage_missing

#------------#
cat(sprintf('Filling missing values was executed successfully at %s...\n', as.character(Sys.time())))
cat('#-------------------------------------------#\n')
#------------#

#---------------------------------------------#
# Part Three -- 15 months forward projection   #
#---------------------------------------------#

## Parallel computing setting.
cl_forward <- makeCluster(numberOfWorkers) # create the clusters.
registerDoParallel(cl_forward) # register the cluster setting to use multicores.


#------------#
cat(sprintf('Start making forward projection at %s...\n',as.character(Sys.time())))
#------------#

## Main Routine.
tic_forward = proc.time()
for (i in 1:18) {
  forward  <- sqldf("with t0 as (
                   select entity_id, max(n_mth) as max
                   from PRODUCTION
                   where comment != 'All Zeros'
                   group by entity_id),

                   t1 as (
                   select a.entity_id, avg(liq) as avg
                   from PRODUCTION a join t0 b on a.entity_id = b. entity_id
                   where n_mth >= max - 6 and comment != 'All Zeros'
                   group by a.entity_id)

                   select entity_id
                   from t1
                   where avg >= 25")

  forward = as.data.table(forward)
  PRODUCTION_last = PRODUCTION[last_prod_date == prod_date, ]
  forward_dt = PRODUCTION_last[entity_id %in% forward[, entity_id], ]

  # forward_dt = forward_dt[first_prod_year != 2016, ]
  # entities whose liq would remain constant.
  forward_const <- sqldf("with t0 as (
                         select entity_id, max(n_mth) as max
                         from PRODUCTION
                         where comment != 'All Zeros'
                         group by entity_id),

                         t1 as (
                         select a.entity_id, avg(liq) as avg
                         from PRODUCTION a join t0 b on a.entity_id = b. entity_id
                         where n_mth >= max - 6 and comment != 'All Zeros'
                         group by a.entity_id)

                         select entity_id
                         from t1
                         where avg < 25 and avg > 0.6667")
  forward_const <- as.data.table(forward_const)
  const_forward_dt = PRODUCTION_last[entity_id %in% forward_const[, entity_id], ]

  # Making forward projection parallelly.
  # Cluster need to be set before.
  temp_PRODUCTION_forward = foreach(j = 1:nrow(forward_dt), .combine = rbind, .packages = 'data.table') %dopar%
    forward_liq_func_v2(j)

  ### data table to store all the entities with constant forward production.
  temp_PRODUCTION_const = const_forward_dt
  const_liq = const_forward_dt[, liq] # use the last availale data as the production.

  temp_PRODUCTION_const[, last_prod_date:= as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_PRODUCTION_const[, n_mth:= (n_mth + 1)]
  temp_PRODUCTION_const[, prod_date:= as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_PRODUCTION_const[, comment:= "Inserted"]
  temp_PRODUCTION_const[, liq:= const_liq]

  # Update the last_prod_date for all the entities.
  PRODUCTION[entity_id %in% forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  PRODUCTION[entity_id %in% const_forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]

  # Append the forward prediction in original data set.
  PRODUCTION = rbindlist(list(PRODUCTION, temp_PRODUCTION_forward,temp_PRODUCTION_const))
  setkey(PRODUCTION, entity_id, basin, first_prod_year, n_mth)
  cat(sprintf('Congratulations! Loop %i runs successfully...\n', i))
  cat(sprintf('Current loop finished at %s...\n', as.character(Sys.time())))
}

setkey(PRODUCTION, entity_id, basin, first_prod_year, n_mth)
toc_forward <- proc.time()
time_usage_forward <- toc_forward - tic_forward
time_usage_forward

stopCluster(cl_forward)

cat(sprintf('Forward projection was executed successfully at %s...\n', as.character(Sys.time())))
cat('#-------------------------------------------#\n')


## ------------------------------------------------------ ##

proj_prod <- PRODUCTION[, .(prod = round(sum(liq)/1000,2)), by = .(prod_date)]
setkey(proj_prod, prod_date)

# Already had historical data before the start_date, only save projected production.
start_date <- paste0("select (max(date) - interval '2 months')::DATE start_date
                      from ei.ei_flat where seriesid = 'PET.MCRFPUS2.M'") %>>%
{dbGetQuery(prod_base, .)}
start_date <- start_date$start_date

proj_prod_subset <- proj_prod[prod_date >= start_date, ]

# ----------------------------------------- #
#             Writing Results               #
# ----------------------------------------- #

cat("Writing required result table...\n")

library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
prod_base_ <- dbConnect(drv, dbname = Prod_DB$PROD_DBNAME, host = Prod_DB$PROD_HOSTNAME,
                        port = 5432, user = Prod_DB$PROD_USERNAME,
                        password = Prod_DB$PROD_PASSWORD, tty = "500")

sqldf(sprintf("insert into cpm.crd_prod_flat
               select 'Proj Existing Prod' prod_type,
                      '%s' basin,
                      prod_date::DATE,
                      prod::NUMERIC,
                      current_timestamp::TIMESTAMP WITHOUT TIME ZONE
               from proj_prod_subset", BASIN),
      dbname = Prod_DB$PROD_DBNAME, host = Prod_DB$PROD_HOSTNAME,
      port = 5432, user = Prod_DB$PROD_USERNAME, password = Prod_DB$PROD_PASSWORD)

cat(sprintf("File has been written successfully...\n"))
cat('#-------------------------------------------#\n\n')