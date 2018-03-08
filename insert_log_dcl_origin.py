# -*- coding: utf-8 -*-
"""
When making prediction for domestic crude oil production,
decline rate need to be calculated. This file contains
necessary functions to calculate decline rate and insert
them into database automatically. Since for all the 21 basins,
it will take more than 7 hours to finish the whole work.
In order to speed up, multi-process would be used.
The function taken in the multi-process is defined here as well.

Author: Xiao Wang
Date: 2016-04-01
"""

import ConnDB
import numpy as np
import time
import threading
import sys
import datetime
import math

# @ 1st function: get basin name list.
def getBasinName():

    basinName = [('SAN JOAQUIN BASIN',),
                 ('CENTRAL KANSAS UPLIFT',),
                 ('GOM - SHELF',),
                 ('FT WORTH BASIN',),
                 ('UINTA',),
                 ('EAST TEXAS BASIN',),
                 ('MISSISSIPPI & ALABAMA GULF COAST BASIN',),
                 ('DENVER JULESBURG',),
                 ('GOM - DEEPWATER',),
                 ('EAGLEFORD',),
                 ('WILLISTON',),
                 ('ARKLA BASIN',),
                 ('PERMIAN BASIN',),
                 ('PACIFIC OFFSHORE',),
                 ('TEXAS & LOUISIANA GULF COAST BASIN',),
                 ('POWDER RIVER',),
                 ('APPALACHIAN',),
                 ('LOS ANGELES BASIN',)]

    return(basinName)


def getProdDate():
    startDate = datetime.datetime(1977, 1, 1)
    currentDate = datetime.datetime.now()

    totalYear = int(math.ceil((currentDate - startDate).days/365.)) + 1

    prod_date = [datetime.datetime(startDate.year + nYear ,1,1).strftime('%Y-%m-%d')
                                   for nYear in range(0, totalYear)]

    return(prod_date)


# 3rd function: insert decline rate values into database.
def insertValue(basinName, start, end):
    # tic = time.clock()  # record start time.

    conn = ConnDB.connectDB('read_write')

    insert_value_sql = """
       with t0 as (
        select a.entity_id, county, basin,
        extract('year' from first_prod_date) as first_prod_year,
            rank() over (partition by a.entity_id order by prod_date) as n_mth,
            prod_date, log(liq/extract(day from prod_date + interval '1 month' - prod_date)+1) as liq
        from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
        where basin in ('%s')
            and liq_cum > 0 and ALLOC_PLUS IN ('Y','X') and
            first_prod_date >= '%s' and first_prod_date < '%s'
            and liq >= 0 and prod_date <= date_trunc('month', current_date) - interval '3 month'
        order by entity_id, prod_date),

        t1 as (
        select a.entity_id, a.basin, a.county, a.first_prod_year, b.n_mth, b.prod_date as prod_date, a.liq as prev_liq, b.liq,
                          round(100*(b.liq - a.liq),2) as dcl
        from t0 a join t0 b on a.entity_id = b.entity_id and a.n_mth = b.n_mth - 1
        where a.liq >= 0 and b.liq >= 0 and round(100*(b.liq - a.liq),2) is not null
        order by a.entity_id, first_prod_year, b.prod_date),

        t2 as (
        select basin, first_prod_year, n_mth, (round(avg(dcl),2) + round(stddev(dcl) ,2)) as high,
        (round(avg(dcl),2) - round(stddev(dcl) ,2)) as low
        from t1
        group by basin, first_prod_year, n_mth
        order by 1, 2, 3),

        t3 as (
        select basin, county, first_prod_year, n_mth, (round(avg(dcl),2) + round(stddev(dcl) ,2)) as high,
        (round(avg(dcl),2) - round(stddev(dcl) ,2)) as low
        from t1
        group by basin, county, first_prod_year, n_mth
        order by 1, 2, 3, 4),

        t4 as (
        insert into cpm.crd_prod_dcl_county
        --insert into cpm.dcl_county_test
        select a.basin, a.county, a.first_prod_year, a.n_mth, round(avg(dcl),2) as avg_dcl
        from t1 a join t3 b on a.basin = b.basin and a.first_prod_year = b.first_prod_year
        and a.n_mth = b.n_mth and a.county = b.county
        where a.dcl <= b. high and a.dcl >= b.low and a.first_prod_year >= 2012
        group by a.basin, a.county, a.first_prod_year, a.n_mth
        order by 1, 2, 3, 4
        )

        insert into cpm.crd_prod_dcl_basin
        --insert into cpm.dcl_basin_test
        select a.basin, a.first_prod_year, a.n_mth, round(avg(dcl),2) as avg_dcl
        from t1 a join t2 b on a.basin = b.basin and a.first_prod_year = b.first_prod_year
        and a.n_mth = b.n_mth
        where a.dcl <= b. high and a.dcl >= b.low
        group by a.basin, a.first_prod_year, a.n_mth
        order by 1, 2, 3;
        """ % (basinName, start, end)

    cursor = conn.cursor()
    cursor.execute(insert_value_sql)
    conn.commit()
    cursor.close()
    conn.close()

    # toc = time.clock()   # record end time.
    # runTime = toc - tic  # calculate running time.

    # print(">> The running time for task %d is %f \n" % (taskNum, runTime))


# 4th function: insert value using multi-processing
def multiInsertValue(basin):
    """
    basin: which basin the data belongs to.
    partNum: how many processes need to be used.
    """
    # tic = time.clock()

    first_prod_date = getProdDate()

    taskList = []
    for i in range(len(first_prod_date) - 1):
        task = threading.Thread(target = insertValue,
            args = (basin, first_prod_date[i], first_prod_date[i + 1]))

        # store all the tasks in one list.
        taskList.append(task)

    # bigBasin = ['PERMIAN BASIN', 'EAGLEFORD', 'WILLISTON', 'APPALACHIAN']

    # if basin in bigBasin:
    for t in taskList:
        t.start()
        t.join()

    # else:
    #     for t in taskList:
    #         t.start()
    #         time.sleep(0.5) # avoid put too much pressure on server.

    #     [t.join() for t in taskList] # end the processes

    # toc = time.clock()
    # elasped = toc - tic

    # print(">> All tasks for basin ( %s ) have completed...\n" %(basin))
    # print("   Elapsed time is %f \n" % (elasped))

if __name__ in '__main__':
    tic = time.clock()

    ind1 = int(sys.argv[1])
    basinList = getBasinName()[ind1][0]
    multiInsertValue(basinList)

    toc = time.clock()
    elasped = toc - tic

    print(">> All tasks for basin ( %s ) have completed...\n" %(basinList))
    print("   Elapsed time is %f \n" % (elasped))
