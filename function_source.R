## @@@ 1st Function
## Define a function to match the decline rate.
## 'dcl' table must be loaded.
find_dcl_factor = function(basin_, first_, month_){
  # Find the log decline rate first.
  dcl_rate = dcl[(first_prod_year == first_ & basin == basin_) & n_mth == month_, avg]/100
  # the decline rate is calculated as r(t) = log(1 + P(t)) - log(1 + P(t - 1))
  # the decline rate factor is calculated as 10^dcl)
  # The next period production is: P(t) = (1 + P(t - 1))*10^(dcl) -1
  dcl_factor = 10^(dcl_rate)
  return(dcl_factor)
}

## @@ 2nd Function
## Define a function for changing data type. Char --> Date
toDate <- function(date_char, j){
  date_res <- as.Date(format(as.Date(date_char) + 32*j, '%Y-%m-01'))
  return(date_res)
}

## @@ 3th Function-
## Define a function for changing data type. Date --> Char
toChar <- function(date_real, j){
  char_res <- as.character(format(as.Date(date_real)+32*j,'%Y-%m-01'))
  return(char_res)
}

## @@ 4th Function
## Define a function to make forward projection
## This function could be executed parallelly.
forward_liq_func <- function(j){
  temp <- forward_dt[j, ]
  temp_entity_id <- temp[,entity_id]
  temp_basin <- temp[, basin]

  if (temp[, first_prod_year] < 1980) {
    first <- 1980 }
  else {
    first <- temp[, first_prod_year]
  }

  ## max month of production in basin where the entity is  and from the year that entity first start producing
  basin_max_mth <- basin_max_mth_tbl[basin == temp_basin & first_prod_year == first, max]

  ## Actual max month of production of the entity
  max_n_mth <- temp[prod_date == last_prod_date[1], n_mth]

  if (max_n_mth >= basin_max_mth) {
    dcl_mth <- basin_max_mth + 1
  } else {
    dcl_mth <- max_n_mth + 1
  }

  # dcl_mth <- (max_n_mth + 1)
  temp_dt = data.table("liq"= (1 + temp[,liq]) * find_dcl_factor(temp_basin, first, dcl_mth) - 1)
  return(temp_dt$liq)
}


forward_liq_func_v2 <- function(j){
  temp <- forward_dt[j, ]
  temp_entity_id <- temp[,entity_id]
  temp_basin <- temp[, basin]

  if (temp[, first_prod_year] < 1980) {
    first <- 1980 }
  else {
    first <- temp[, first_prod_year]
  }

  ## max month of production in basin where the entity is  and from the year that entity first start producing
  basin_max_mth <- basin_max_mth_tbl[basin == temp_basin & first_prod_year == first, max]

  ## Actual max month of production of the entity
  max_n_mth <- temp[prod_date == last_prod_date[1], n_mth]

  if (max_n_mth >= basin_max_mth) {
    dcl_mth <- basin_max_mth + 1
  } else {
    dcl_mth <- max_n_mth + 1
  }

  proj_liq = (1 + temp[,liq]) * find_dcl_factor(temp_basin, first, dcl_mth) - 1
  # dcl_mth <- (max_n_mth + 1)
  temp_dt = temp
  # entity_id, basin, first_prod_year are the same.
  temp_dt[, last_prod_date := as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_dt[, n_mth := (n_mth + 1)]
  temp_dt[, prod_date := as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_dt[, comment := "Inserted"]
  temp_dt[, liq := proj_liq]
  return(temp_dt)
}


## @@ 5th Function
## calculate the moving average
Moving_Avg <- function(data, interval){
  n = length(data)
  m = interval
  SMA = data.frame('SMA' = rep(0, (n - m + 1)))

  for(i in n:1){
    if((i - m + 1) <= 0){
      break
    } else{
      avg <- mean(data[i: (i - m + 1)])
      j = (n - i + 1)
      SMA[j,1] = avg
    }
  }
  SMA = SMA[c((n - m + 1):1),]

  return(as.data.frame(SMA))
}

## @@ 6th Function
### The function will return a data.table containing basin DI.
partition_load <-function(conn, basin_set, part_num, tbl_name = 'cpm.crd_prod_base'){
  ### basin_set: the basins that needed.
  ### part_num: how many parts need to be partitioned.

  ## load required library first.
    # if loaded before, this line can be commented.
  # library(pipeR)
  # library(data.table)
  ###

  first_prod_year <- sprintf("select distinct(first_prod_year) from %s
                              where basin in %s
                             order by first_prod_year", tbl_name, basin_set) %>>%
                             {dbGetQuery(conn, .)}
  n = nrow(first_prod_year)
  PARTNUM <- part_num
  ind <- seq(from = 1, to = n,  by = (n - 1)/PARTNUM) # partition the table into 5 parts
  thres <- first_prod_year[ind[2:PARTNUM],] # threshold for making partitions.

  # create an empty data table to hold all values.
  result_table <- sprintf("select * from %s limit 0", tbl_name) %>>%
                          {dbGetQuery(conn, .)} %>>%
                          {as.data.table(.)}

  for(i in 1:(PARTNUM)){
    if(i == 1){
      query <- sprintf("select * from %s
                        where basin in %s and first_prod_year < %s", tbl_name, basin_set, thres[i])
    } else if(i == (PARTNUM)){
      query <- sprintf("select * from %s
                       where basin in %s and first_prod_year >= %s", tbl_name, basin_set, thres[i - 1])
    } else{
      query <- sprintf("select * from %s
                       where basin in %s and first_prod_year >= %s
                       and first_prod_year < %s", tbl_name, basin_set,
                       thres[i - 1], thres[i])
    }
    ## using data.table and rbindlist for fast table binding.
    partition <- dbGetQuery(conn, query) %>>% as.data.table()
    result_table <- rbindlist(list(result_table, partition))
  }
  return(result_table)
}


## @@ 7th Function
## This function return a data.table containing all the updated entries which used to be zeros.
## prod_tbl should be the production table which needs to be updated, like: ndakota, permian etc.
## basin_max_mth_tbl_ contains info about max production month.

filling_zero <- function(i, prod_tbl){
  #choose prod data for past 6 month
  temp <- prod_tbl[entity_id == zero[i,entity_id],]
  temp_entity_id <- temp[1,entity_id]
  temp_basin <- temp[1,basin]

  if (temp[1,first_prod_year] < 1980) {
    first <- 1980 } else {
      first <- temp[1,first_prod_year]
    }

  ## max month of production in basin where the entity is and from the year that entity first start producing
  basin_max_mth <- basin_max_mth_tbl[basin == temp_basin & first_prod_year == first, max]

  ## Actual max month of production of the entity
  max_n_mth <- temp[prod_date == last_prod_date[1], n_mth]

  ## find the max month of dcl
  if (max_n_mth >= basin_max_mth) {
    dcl_mth <- basin_max_mth
  } else {
    dcl_mth <- max_n_mth
  }

  if (temp[n_mth == (max_n_mth - 1),liq] == 0) {

    if (temp[n_mth == (max_n_mth - 2),liq] == 0) {

      if (temp[n_mth == (max_n_mth - 3),liq] == 0) {

        temp_dt <- data.table('entity_id' = temp_entity_id,
                              'n_mth' = max_n_mth - 3,
                              'liq' = 0,
                              'comment' = "All Zeros")
      } else {

        temp_liq_lag_2 = (1 + temp[n_mth == (max_n_mth - 3), liq]) * find_dcl_factor(temp_basin, first, dcl_mth - 2) - 1
        temp_liq_lag_1 = (1 + temp_liq_lag_2) * find_dcl_factor(temp_basin, first, dcl_mth - 1) - 1
        temp_liq_lag_0 = (1 + temp_liq_lag_1) * find_dcl_factor(temp_basin, first, dcl_mth) - 1

        temp_dt <- data.table('entity_id' = rep(temp_entity_id,3),
                              'n_mth' = c((max_n_mth - 2), (max_n_mth - 1), max_n_mth),
                              'liq' = pmax(c(temp_liq_lag_2,temp_liq_lag_1, temp_liq_lag_0), 0),
                              'comment' = rep("Updated",3))
      }
    } else {

      temp_liq_lag_1 = (1 + temp[n_mth == (max_n_mth - 2), liq]) * find_dcl_factor(temp_basin, first, dcl_mth - 1) - 1
      temp_liq_lag_0 = (1 + temp_liq_lag_1) * find_dcl_factor(temp_basin, first, dcl_mth) - 1
      temp_dt <- data.table('entity_id' = rep(temp_entity_id,2),
                            'n_mth' = c((max_n_mth - 1), max_n_mth),
                            'liq' = pmax(c(temp_liq_lag_1, temp_liq_lag_0), 0),
                            'comment' = rep("Updated",2))
    }

  } else {
    temp_liq_lag_0 = (1 + temp[(n_mth == (max_n_mth - 1)), liq]) * find_dcl_factor(temp_basin, first, dcl_mth) - 1
    temp_dt <- data.table('entity_id' = temp_entity_id,
                          'n_mth' = max_n_mth,
                          'liq' = max(temp_liq_lag_0, 0),
                          'comment' = "Updated")
  }
  return(temp_dt)
}

## @8th Function
## This function is used to update values in the production table.
## After parallelly computing the entries supposed to be zero,
## updated values need to be merged in the orignal table.


update_table <- function(orig_tbl, update_val_tbl){
  ## orig_table: original production table, like: ndakota, permian etc.
  ## update_val_tbl: this table is compueted and returned by filling_zero.

  zero <- update_val_tbl[comment == 'All Zeros', ]  # zero part
  updated <- update_val_tbl[comment == 'Updated', ] # updated part
  orig_tbl[entity_id %in% zero[, entity_id], comment:= 'All Zeros']
  update_df <- sqldf("select a.*, b.liq liq_b, b.comment comment_b from orig_tbl a left join updated b
                     on a.entity_id = b.entity_id and a.n_mth = b.n_mth")
  update_dt <- as.data.table(update_df)
  update_dt[is.na(comment_b), comment_b := comment]
  update_dt[is.na(liq_b), liq_b := liq]
  update_dt[, comment:=comment_b]
  update_dt[, liq:=liq_b]
  update_dt[, comment_b:=NULL]
  update_dt[, liq_b:=NULL]

  # return the updated orignal table.
  # It's better to overwrite the previous one.
  return(update_dt)
}

## @9th Function
## This function retuens a data.table containing inserted missing values.
## By executing this function, filling missing values could be made parallelly.

filling_missing <- function(i, cutoff_date){
  temp <- missing[i,]
  temp_entity_id <- temp[, entity_id]
  temp_basin <- temp[, basin]

  if (temp[1, first_prod_year] < 1980) {
    first <- 1980 } else {
      first <- temp[1, first_prod_year]
    }

  ## max month of production in basin where the entity is  and from the year that entity first start producing
  basin_max_mth <- basin_max_mth_tbl[basin == temp_basin & first_prod_year == first, max_new]

  ## Actual max month of production of the entity
  max_n_mth <- temp[prod_date == last_prod_date[1], n_mth]

  temp_hold_dt <- temp[entity_id == 0, ]
  ## find the max month of dcl
  if (max_n_mth >= basin_max_mth) {
    dcl_mth <- basin_max_mth

    # j = 0
    for(j in 1:5)
    {
      if(toDate(temp[, prod_date], j) > cutoff_date)
      {
        break
      }
      if(toDate(temp[, prod_date], j) <= cutoff_date)
      {

        if(j == 1) {
          temp_liq = (1 + temp[, liq]) *find_dcl_factor(temp[, basin], first, dcl_mth) - 1
          temp_liq_last = temp_liq
        } else {
          temp_liq = (1 + temp_liq_last) * find_dcl_factor(temp[, basin], first, dcl_mth) - 1
          temp_liq_last = temp_liq
        }

        # Create a temporary data table to store the generated row.
        temp_dt = data.table(
          "entity_id" = temp_entity_id,
          "basin" = temp_basin,
          "first_prod_year" = temp[,first_prod_year],
          "last_prod_date" = toChar(temp[,last_prod_date], j),
          "n_mth" = (temp[,n_mth] + j),
          "prod_date" = toChar(temp[, prod_date], j),
          "liq" = max(temp_liq, 0),
          "comment" = "Inserted")
        temp_hold_dt = rbindlist(list(temp_hold_dt, temp_dt))
      }
    }
    temp_hold_dt[, last_prod_date := as.character(cutoff_date)]

  } else {
    dcl_mth <- max_n_mth

    # j = 1
    for(j in 1:5)
    {
      if(toDate(temp[, prod_date], j) > cutoff_date)
      {
        break
      }
      if(toDate(temp[, prod_date], j) <= cutoff_date)
      {
        if(j == 1) {
          temp_liq = (1 + temp[,liq]) * find_dcl_factor(temp_basin, first, dcl_mth + j) - 1
          temp_liq_last = temp_liq
        } else {
          temp_liq = (1 + temp_liq_last) * find_dcl_factor(temp_basin, first, dcl_mth + j) - 1
          temp_liq_last = temp_liq
        }

        temp_dt = data.table(
          "entity_id" = temp_entity_id,
          "basin" = temp_basin,
          "first_prod_year" = temp[,first_prod_year],
          "last_prod_date" = toChar(temp[,last_prod_date], j),
          "n_mth" = (temp[,n_mth] + j),
          "prod_date" = toChar(temp[,prod_date], j),
          "liq" = max(temp_liq, 0),
          "comment" = "Inserted")
        temp_hold_dt = rbindlist(list(temp_hold_dt, temp_dt))
      }
    }
    temp_hold_dt[, last_prod_date := as.character(cutoff_date)]
  }
  return(temp_hold_dt)
}

## @10th Function. Output basin info of program.
initializing <- function(BASIN_SET, PROD_TBL, DCL_TBL, BASIN_MAX_MTH_TBL, numberOfWorkers){
  cat('## -------------------------------------------------## \n')
  cat(sprintf("  Crude Production for %s Initializing...\n\n", BASIN_SET))
  cat("   Please Checking Basic Info Listed:\n\n")
  cat(sprintf("   * BASIN_SET: %s\n", BASIN_SET))
  cat(sprintf("   * PROD_TBL: %s\n", PROD_TBL))
  cat(sprintf("   * DCL_TBL: %s\n", DCL_TBL))
  cat(sprintf("   * BASIN_MAX_MTH_TBL: %s\n", BASIN_MAX_MTH_TBL))
  cat(sprintf("   * NUMBER_OF_WORKERS: %d\n\n", numberOfWorkers))
  cat('## -------------------------------------------------## \n')
}

## @11th function

filling_zero_v_2 <- function(i, prod_tbl){
  #choose prod data for past 6 month
  # prod_tbl = PRODUCTION

  temp <- prod_tbl[entity_id == zero[i, entity_id],]
  temp_entity_id <- temp[1,entity_id]
  temp_basin <- temp[1,basin]

  if (temp[1,first_prod_year] < 1980) {
    first <- 1980 } else {
      first <- temp[1,first_prod_year]
    }

  ## max month of production in basin where the entity is and from the year that entity first start producing
  basin_max_mth <- basin_max_mth_tbl[basin == temp_basin & first_prod_year == first, max_new]

  ## Actual max month of production of the entity
  max_n_mth <- temp[prod_date == last_prod_date[1], n_mth]

  ## find the max month of dcl
  if (max_n_mth >= basin_max_mth) {
    dcl_mth <- basin_max_mth
  } else {
    dcl_mth <- max_n_mth
  }

  if (max_n_mth == 2)
  {

    if (temp[n_mth == (max_n_mth - 1),liq] == 0)
    {

      temp_dt <- data.table('entity_id' = temp_entity_id,
                            'n_mth' = max_n_mth - 1,
                            'liq' = 0,
                            'comment' = "All Zeros")
    } else
    {
      temp_liq_lag_0 = (1 + temp[n_mth == (max_n_mth - 1), liq]) * find_dcl_factor(temp_basin, first, dcl_mth) - 1, 2)

      temp_dt <- data.table('entity_id' = temp_entity_id,
                            'n_mth' = max_n_mth,
                            'liq' = pmax(temp_liq_lag_0, 0),
                            'comment' = "Updated")
    }
  } else if (max_n_mth == 3)
  {

    if (temp[n_mth == (max_n_mth - 1),liq] == 0)
    {
      if (temp[n_mth == (max_n_mth - 2),liq] == 0)
      {
        temp_dt <- data.table('entity_id' = temp_entity_id,
                              'n_mth' = max_n_mth - 2,
                              'liq' = 0,
                              'comment' = "All Zeros")
      } else
      {
        temp_liq_lag_1 = (1 + temp[n_mth == (max_n_mth - 2), liq]) * find_dcl_factor(temp_basin, first, dcl_mth - 1) - 1, 2)
        temp_liq_lag_0 = temp_liq_lag_1 * find_dcl_factor(temp_basin, first, dcl_mth) - 1, 2)

        temp_dt = data.table('entity_id' = rep(temp_entity_id, 2),
                             'n_mth' = c((max_n_mth - 1), max_n_mth),
                             'liq' = pmax(c(temp_liq_lag_1, temp_liq_lag_0), 0),
                             'comment' = rep("Updated",2))
      }
    } else
    {
      temp_liq_lag_0 = (1 + temp[(n_mth == (max_n_mth - 1)), liq]) * find_dcl_factor(temp_basin, first, dcl_mth) - 1
      temp_dt <- data.table('entity_id' = temp_entity_id,
                            'n_mth' = max_n_mth,
                            'liq' = max(temp_liq_lag_0, 0),
                            'comment' = "Updated")
    }
  } else
  {
    if (temp[n_mth == (max_n_mth - 1),liq] == 0)
    {
      if (temp[n_mth == (max_n_mth - 2),liq] == 0)
      {
        if (temp[n_mth == (max_n_mth - 3),liq] == 0)
        {
          temp_dt <- data.table('entity_id' = temp_entity_id,
                                'n_mth' = max_n_mth - 3,
                                'liq' = 0,
                                'comment' = "All Zeros")
        } else
        {
          temp_liq_lag_2 = (1 + temp[n_mth == (max_n_mth - 3), liq]) * find_dcl_factor(temp_basin, first, dcl_mth - 2) - 1
          temp_liq_lag_1 = (1 + temp_liq_lag_2) * find_dcl_factor(temp_basin, first, dcl_mth - 1) - 1
          temp_liq_lag_0 = (1 + temp_liq_lag_1) * find_dcl_factor(temp_basin, first, dcl_mth) - 1
          temp_dt <- data.table('entity_id' = rep(temp_entity_id,3),
                                'n_mth' = c((max_n_mth - 2), (max_n_mth - 1), max_n_mth),
                                'liq' = pmax(c(temp_liq_lag_2,temp_liq_lag_1, temp_liq_lag_0), 0),
                                'comment' = rep("Updated",3))
        }
      } else
      {
        temp_liq_lag_1 = (1 + temp[n_mth == (max_n_mth - 2), liq]) * find_dcl_factor(temp_basin, first, dcl_mth - 1) - 1
        temp_liq_lag_0 = (1 + temp_liq_lag_1) * find_dcl_factor(temp_basin, first, dcl_mth) - 1
        temp_dt <- data.table('entity_id' = rep(temp_entity_id, 2),
                              'n_mth' = c((max_n_mth - 1), max_n_mth),
                              'liq' = pmax(c(temp_liq_lag_1, temp_liq_lag_0), 0),
                              'comment' = rep("Updated",2))
      }
    } else
    {
      temp_liq_lag_0 = (1 + temp[(n_mth == (max_n_mth - 1)), liq]) * find_dcl_factor(temp_basin, first, dcl_mth) - 1
      temp_dt <- data.table('entity_id' = temp_entity_id,
                            'n_mth' = max_n_mth,
                            'liq' = max(temp_liq_lag_0, 0),
                            'comment' = "Updated")
    }
  }

  return(temp_dt)
}

