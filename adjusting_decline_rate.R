### Loading required packages.
library(rJava)
library(RJDBC)
library(sqldf)
library(plyr)
library(foreach)
library(doParallel)
library(data.table)
library(pipeR)

# Load source file which contains connection info.
source("/home/CL_US_Crude_Prod/us_crude_prod/getDBInfo.R")
# source("/home/infinitecycle/Desktop/GoogleDrive/Programming/crude_production/us_crude_prod/getDBInfo.R")

## Set the driver of the PostgreSQL, the driver file (postgresql-9.4.1207.jar) must be downloaded first.
# pgsql <- JDBC("org.postgresql.Driver", "/home/infinitecycle/Desktop/GoogleDrive/Programming/crude_production/postgresql-9.4.1207.jar")
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

options("scipen"=100)
options(stringsAsFactors = F)

#########################################################################################################################

print(Sys.time())

dcl_all <- dbGetQuery(prod_base, "select *
                      from cpm.crd_prod_dcl_basin") %>>%
                      as.data.table()

## distinct first production year
first_prod_year <- sqldf("select distinct basin,
                                 first_prod_year
                         from dcl_all
                         where first_prod_year >= 1980
                         order by 1, 2") %>>%
                         as.data.table()

## distinct basins
basin_all <- sqldf("select distinct basin from dcl_all order by 1") %>>% as.data.table()

## max decline rate for each basin each year
basin_max_mth_table <- sqldf("select basin,
                                     first_prod_year,
                                     max(n_mth) as max,
                                     max(n_mth) + 20 as max_new
                             from dcl_all
                             group by basin, first_prod_year
                             order by 1, 2") %>>%
                             as.data.table()


## latest_prod_year
latest_year <- dbGetQuery(prod_base, "select extract(year from date_trunc('month', current_date - interval '3 month')::DATE)")

# latest_prod_month
latest_mth <- dbGetQuery(prod_base, "select extract(month from date_trunc('month', current_date - interval '3 month')::DATE)")

# create a table of all # of mth of prod for each start year, and basin
mths <- as.data.frame(matrix(nrow = 0, ncol = 3));

# loop through basin
for (i in (1:nrow(basin_all))) {
  temp <- first_prod_year[basin == basin_all[i,basin], ]
  # loop through first_prod_year
  for (j in (1: nrow(temp))) {
    t <- basin_max_mth_table[basin == basin_all[i, basin] & first_prod_year == temp[j, first_prod_year], max]
    # as.numeric((latest_year - temp$first_prod_year[j])*12 + latest_mth)
    mths <- rbind(mths, cbind(rep(basin_all[i, basin], t-1), rep(temp[j, first_prod_year], t-1), c(2:t)))
  }
}

colnames(mths)<-c('basin', 'first_prod_year', 'n_mth');
mths$first_prod_year <- as.numeric(mths$first_prod_year)
mths$n_mth <- as.numeric(mths$n_mth)

# missing decline rate
missing_dcl <- sqldf("select a.*
                     from mths a
                     left join dcl_all b
                        on a.basin = b.basin
                        and a.first_prod_year = b.first_prod_year
                        and a.n_mth = b.n_mth
                     where b.basin is null
                     order by 1, 2, 3")

if (nrow(missing_dcl) == 0) {
  print("No missing decline rate")
} else {
    for (l in (1: nrow(missing_dcl))) {

    n <- nrow(dcl_all)
    dcl_all[n+1, 1] <- missing_dcl[l,1]
    dcl_all[n+1, 2] <- missing_dcl[l,2]
    dcl_all[n+1, 3] <- missing_dcl[l,3]

    sql <- sprintf("select avg(avg) as avg
                   from dcl_all
                   where basin = '%s' and first_prod_year >= '%s' - 3 and n_mth = '%s'",
                   missing_dcl[l,1], missing_dcl[l,2], missing_dcl[l,3])

    dcl_all[n+1, 4] <- sqldf(sql)
  }
}


## loop through all basins
for (k in (1:nrow(basin_all))) {

  basin_ <- basin_all[k, basin]

  ## all first prod year for basin
  years <- first_prod_year[basin == basin_, first_prod_year]


  ## forward 15 month
  for (h in (1:length(years))) {

    temp <- dcl_all[basin == basin_ & first_prod_year == years[h], ]
    #first prod year in temp
    year <- temp[1, first_prod_year]
    #max mth produced in temp
    m <- max(temp$n_mth)


    sql <- sprintf("select basin, '%s' as first_prod_year, n_mth, avg(avg) as avg
                   from dcl_all
                   where basin = '%s' and first_prod_year >= ('%s' - 3)
                  and first_prod_year < '%s' and n_mth > '%s' and n_mth <= '%s'
                   group by basin, n_mth
                   order by 1,2,3",year, basin_, year, year, m, m + 24)

    dcl_ext <- sqldf(sql)

    if (nrow(dcl_ext) < 20) {
      sql1 <- sprintf("select basin, first_prod_year, n_mth + 24 as n_mth, avg
                      from dcl_all
                      where basin = '%s' and first_prod_year = '%s' and n_mth > '%s' and n_mth <= '%s'
                      group by basin, n_mth
                      order by 1,2,3", basin_, year, m - 24, m )

      dcl_ext1 <- sqldf(sql1)

      dcl_all <- rbind(dcl_all, dcl_ext1)
    }else {
      dcl_all <- rbind(dcl_all, dcl_ext)
    }
  }
}


maxFirstProdYear = dcl_all[,.(max_first_prod_year = max(first_prod_year)), by = .(basin)]

# Find basin whose max first_prod_year is not 2016
notLatest = maxFirstProdYear[max_first_prod_year != max(max_first_prod_year), ]

if(nrow(notLatest) != 0){
  # update max first_prod_year
  updateMaxFirstProdYear = data.table(basin = notLatest[, basin],
                                      first_prod_year = max(maxFirstProdYear[, max_first_prod_year]),
                                      max = 2,
                                      max_new = 24)

  basin_max_mth_table = rbindlist(list(basin_max_mth_table, updateMaxFirstProdYear))
  setkey(basin_max_mth_table, basin, first_prod_year)


  # ------- #
  new_prod_basin_county = dbGetQuery(prod_base, "select * from cpm.crd_prod_new_prod_info")

  new_prod_basin_county <- as.data.table(new_prod_basin_county)
  new_prod_basin_county[, month := as.numeric(substr(prod_date, 6, 7))]
  new_prod_basin_county[, year := as.numeric(substr(prod_date, 1, 4))]

  new_prod_ratio <- new_prod_basin_county[, .(m_year_total = sum(total_new_prod)), by = .(basin, county)]
  new_prod_ratio[, basin_total := sum(m_year_total), by = .(basin)]
  new_prod_ratio[, ratio := round(m_year_total/basin_total, 4)]
  new_prod_ratio[, county_num := .N, by = .(basin)]


  select_county <- new_prod_ratio[ratio >= 0.04, ]  # select counties whose new prod account for more than 4%.
  select_county[, total_ratio := sum(ratio), by = .(basin)] # total ratio of the selected counties.
  select_county[, select_county_num := .N, by = .(basin)] # how many counties were selected
  select_county[, county_ratio := select_county_num/county_num] # proportion of selected counties.



  county_set <- select_county[, county]

  select_county_dcl <- dbGetQuery(prod_base, "select * from cpm.crd_prod_dcl_county limit 0") %>>% as.data.table()

  for(i in 1:length(county_set)){
    temp_county = county_set[i]
    new_prod_dcl <- paste0(sprintf("select * from cpm.crd_prod_dcl_county
                                   where county = '%s'", temp_county)) %>>%
                                   {dbGetQuery(prod_base, .)} %>>% as.data.table()

    select_county_dcl <- rbindlist(list(select_county_dcl, new_prod_dcl))
  }

  dcl_2015 <- select_county_dcl[first_prod_year == 2015 & n_mth > 8 & n_mth <= 24, ]
  dcl_2016 <- select_county_dcl[first_prod_year == 2016 & n_mth <= 8, ]


  # Taking average
  dcl_2015_part <- dcl_2015[, .(county_avg = mean(avg), county_max = max(avg),
                                county_std = sd(avg)), by = .(basin, n_mth)]
  dcl_2015_part[, county_mix := county_avg]
  dcl_2015_part[, county_avg_plus_std := county_avg]


  dcl_2016_part <- dcl_2016[, .(county_avg = mean(avg), county_max = max(avg),
                                county_std = sd(avg)), by = .(basin, n_mth)]
  dcl_2016_part[, county_mix := county_avg]
  dcl_2016_part[n_mth <= 4, county_mix := county_max]
  dcl_2016_part[, county_avg_plus_std := (county_avg + county_std)]


  dcl_basin <- dbGetQuery(prod_base, "select * from cpm.crd_prod_dcl_basin
                          where basin in ('TEXAS & LOUISIANA GULF COAST BASIN',
                                      'EAGLEFORD',
                                      'PERMIAN BASIN',
                                      'WILLISTON'                       ,
                                      'SAN JOAQUIN BASIN',
                                      'DENVER JULESBURG',
                                      'POWDER RIVER',
                                      'MISSISSIPPI & ALABAMA GULF COAST BASIN',
                                      'UINTA',
                                      'LOS ANGELES BASIN',
                                      'EAST TEXAS BASIN',
                                      'FT WORTH BASIN',
                                      'APPALACHIAN',
                                      'ARKLA BASIN',
                                      'PACIFIC OFFSHORE',
                                      'CENTRAL KANSAS UPLIFT',
                                      'GOM - DEEPWATER',
                                      'GOM - SHELF')
                          and first_prod_year <= 2016 and first_prod_year >= 2012")

  dcl_basin <- as.data.table(dcl_basin)

  dcl_basin_year_avg = dcl_basin[n_mth <= 24, .(basin_year_avg = mean(avg)), by = .(basin, n_mth)]
  setkey(dcl_basin_year_avg, basin, n_mth)

  new_prod_dcl <- rbindlist(list(dcl_2015_part, dcl_2016_part))
  setkey(new_prod_dcl, basin, n_mth)

  new_prod_dcl <- new_prod_dcl[dcl_basin_year_avg]  # left join the dcl rate

  new_prod_dcl[, county_avg_p_hstd := county_avg + 0.5*county_std]

  # specifically for PACIFIC OFFSHORE.
  new_prod_dcl[basin == 'PACIFIC OFFSHORE', county_avg := basin_year_avg]
  new_prod_dcl[basin == 'PACIFIC OFFSHORE', county_avg_plus_std := basin_year_avg]
  new_prod_dcl[basin == 'PACIFIC OFFSHORE', county_avg_p_hstd := basin_year_avg]
  new_prod_dcl[basin == 'PACIFIC OFFSHORE', county_max := basin_year_avg]
  new_prod_dcl[basin == 'PACIFIC OFFSHORE', county_mix := basin_year_avg]
  new_prod_dcl[basin == 'PACIFIC OFFSHORE', county_std := 0]


  setcolorder(new_prod_dcl, c( 'basin', 'n_mth', 'county_avg', 'county_std', 'county_avg_plus_std',
                               'county_avg_p_hstd','county_max', 'county_mix', 'basin_year_avg'))


  dcl_dt_missing_year = new_prod_dcl[, .(basin, first_prod_year = updateMaxFirstProdYear[1, first_prod_year], n_mth, avg = basin_year_avg)]
  dcl_dt_missing_year_part = dcl_dt_missing_year[basin %in% updateMaxFirstProdYear[, basin], ]


  dcl_all <- rbindlist(list(dcl_all, dcl_dt_missing_year_part))
} else{
  dcl_all <- as.data.table(dcl_all)
}

setkey(dcl_all, basin, first_prod_year)

# -----------------#
# Insert the all the dcl rate back into the database


library(RPostgreSQL)
library(sqldf)

drv <- dbDriver("PostgreSQL")
prod_base_ <- dbConnect(drv, dbname = Prod_DB$PROD_DBNAME, host = Prod_DB$PROD_HOSTNAME,
                       port = 5432, user = Prod_DB$PROD_USERNAME, password = Prod_DB$PROD_PASSWORD, tty="500")

cat(sprintf("%s Inserting adjusted decline rate: Initiating...\n", Sys.time()))

# Fast insert all the decline rate values into cpm.crd_prod_dcl_basin table.
sqldf("insert into cpm.crd_prod_adj_dcl_basin
      select * from dcl_all",
      dbname = Prod_DB$PROD_DBNAME, host = Prod_DB$PROD_HOSTNAME,
      port = 5432, user = Prod_DB$PROD_USERNAME, password = Prod_DB$PROD_PASSWORD)

sqldf("insert into cpm.crd_prod_basin_max_mth
      select * from basin_max_mth_table",
      dbname = Prod_DB$PROD_DBNAME, host = Prod_DB$PROD_HOSTNAME,
      port = 5432, user = Prod_DB$PROD_USERNAME, password = Prod_DB$PROD_PASSWORD)


cat("Insert all the adjusted decline rate values successfully...\n")
