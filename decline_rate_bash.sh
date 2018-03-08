#!/usr/bin/bash

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 0
## Insert decline rate for SAN JOAQUIN BASIN

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 1
## Insert decline rate for CENTRAL KANSAS UPLIFT

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 2
## Insert decline rate for GOM - SHELF

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 3
## Insert decline rate for FT WORTH BASIN

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 4
## Insert decline rate for UINTA

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 5
## Insert decline rate for EAST TEXAS BASIN

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 6
## Insert decline rate for MISSISSIPPI & ALABAMA GULF COAST BASIN

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 7
## Insert decline rate for DENVER JULESBURG

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 8
## Insert decline rate for GOM - DEEPWATER

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 9
## Insert decline rate for EAGLEFORD

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 10
## Insert decline rate for WILLISTON

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 11
## Insert decline rate for ARKLA BASIN

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 13
## Insert decline rate for PACIFIC OFFSHORE

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 14
## Insert decline rate for TEXAS & LOUISIANA GULF COAST BASIN

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 15
## Insert decline rate for POWDER RIVER

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 16
## Insert decline rate for APPALACHIAN

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 17
## Insert decline rate for LOS ANGELES BASIN

python /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/insert_log_dcl_origin.py 12
## Insert decline rate for PERMIAN BASIN

echo "Calculating and inserting raw decline rate finished..."

# echo "Start adjusting decline rate..."
# After the original decline rates have been calculated,
# further adjustments need to be done by running
# adjusting_decline_rate.R; adjusted decline rates will be
# inserted back into the database.

Rscript /home/CL_US_Crude_Prod/us_crude_prod/Calculate_Decline_Rate/adjusting_decline_rate.R

