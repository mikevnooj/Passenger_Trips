# Samuel Carter
# Data Analyst, IndyGo
# September 23, 2019

### --- Purpose --- ###

# This is a repository of adhoc ridership request.

### --- Notes --- ###

#### --- Revision History --- ####

# 9/23/19 - Request from AD: BYD ridership over 9/21/19 and 9/22/19 (not just route 90).
#         - AD also wants ridership by bus and block.

# 9/26/19 - Request from RG for boardings/alightings at Central and 25th from
#           9/1 to 9/25 for routes 19 and 39

# 10/1/19 - Researched boardings for buses 1986 and 1988 from 9/25-9/28, roughly.

# 10/2/19 - Pulled 1986 from yesterday for Brandon Parks from CVT. Validate data.

# 10/3/19 - Pulled date, block, vehicle data for local route 90 for AD.

# 11/5/19 - Pulled date, block, vehicle data for local route 90 for AD (for October data, up until 10/27)

# 11/12/19- Pulled Red Line counts for Sunday (11/10) between 4pm - 10pm for Annette, Operations.

# 11/15/19- Pulled end-of-line boarding numbers since pick for Aaron Vogel.

# 12/3/19 - Pulled Average Daily Boardings for stops north of 91st street for BL.

# 12/4/19 - Pulled Average Daily Boardings Greenwood Mall Park and Ride for BL.

# 12/9/19 - Pulled Daily/Monthly Ridership for DTC since opening to current for Ed, Cheryl.
#           NOTE: should probably mutate for "Activity" or something (combo of boardings and alightings...)
#           OR.... just pick whichever one is higher...
#           ALSO..... could be that there are bad BRT APCs that only show bad alightings....
#           Need to verify.... alightings are very high...
#           UPDATE: I came to the conclusion we had better alighting data with the Avail install.

# 12/18/19- Pulled November boardings at 38th street for Brandon from Public Affairs.

# 12/19/19- Pulled daily ridership from November for Justin.

# 1/2/20 - Began pulling ridership information from New Year’s Eve (8pm – 2am) for Operations.

# 1/16/20 - JS wants ridership for stops 10304 and 10353 for the month of December.

# 1/22/20 - Ryan G wants average daily boardings / alightings from Sep - Dec for these stops:

#           12460
#           51649
#           51704
#           51659
#           51658
#           51657

# 1/20/20 - Pulled stop-level data for Justin S.

# 3/~/20 - Pulled recent ridership (migrated to own script)

# 3/25/20 - Pulled ridership per trip..

# 4/21/20 - John Marron wants boardings/alightings per station for January and February.

# 4/24/20 - Public Affairs wants to know high ridership stations. Get ranking
#           of station boardings per month.

# 5/1/20 - Marion County Public Health wants to know average daily ridership per route
#          from March 1 – March 14 and then March 29 – April 25.

# 5/18/20 - Service Planning wants average weekday boardings for stops 11570, 11540,
#           maybe 52097 Westfield/College WB FS and IB & OB at Westfield and College.
#           Do 9/2019 to 3/2020 and 3/2020 to present.

# 5/26/20 - Ops wants to know peak boardings per hour for route 901. Run it since May 4th

# 5/27/20 - Aaron wants to know highest ridership day on Red Line since Jan 1.

# 6/26/20 - Thomas Coon wants to know average daily ridership and ADR by hour for each of
#           the following stops: 52263; 52264; 52265; 52266; 52302; 52303; 52304

# 7/6/20 - Debra wants ten busiest platforms. Get this data since April 1st.

# 2020/09/09
# briometrix wants data for a few stops

# 2020/09/10
# annette wants to know boardings and alightings for 66th sb

# 2020/09/15
# aaron wants to know highest ridership day from jan 19ish to march

# 2020/10/01
# mr. roth wants monthly total wheelchair users, by route, excluding route 90

# 2020/10/06
# aaron wants to know apc count by day by vehicle for June 2020

# 2020/10/19
# Aletra wants red line ridership by hour for each month Feb thru Sept

# 2020/10/29
# Service Planning would like a new Average Daily Boarding and Alighting dataset for 2020
# two sets, pre-cov and post-cov
# Jan - March 14th is pre
# June - October 1 is post
# also want september thru march 14th by stop and by stop by route
# also want 2018 by stop by route


# 2021/02/23
# MPO Wants weekly total by stop by route for 16, 31, and 902
# 2 ranges
# pre-cov
# 2019/09/1 thru 2020/03/14
# and
# 2020/09/01 thru 2021/02/01


### --- Libraries --- ###

library(tidyverse)

# Data from Steve's temp database

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_BYD <- tbl(con, "Apc_Data") %>%
  filter(MDCID >= 1950, MDCID < 2000, 
  GPS_Time >= "2019-09-21", GPS_Time <= "2019-09-23") %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_BYD$Date <- as.Date(str_sub(Apc_BYD$GPS_Time, 1, 10))

Apc_BYD$Clock_Time <- str_sub(Apc_BYD$GPS_Time, 12, 19)

Apc_BYD$DateTest <- ifelse(Apc_BYD$Clock_Time < 
                                  "03:00:00", 1, 0)





# now change Transit Day based on DateTest

Apc_BYD$Transit_Day_Unix <- ifelse(Apc_BYD$DateTest == 1,
                                        lubridate::as_date(Apc_BYD$Date - 1),
                                        Apc_BYD$Date)

# add two dates together

Apc_BYD$Epoch_Date <- as.Date("1970-01-01")

Apc_BYD$Transit_Day <- Apc_BYD$Epoch_Date + lubridate::days(Apc_BYD$Transit_Day_Unix)

# So now let's get boardings by transit day

Apc_BYD %>%
  filter(MDCID >= 1950, MDCID < 2000) %>%
  group_by(Transit_Day) %>%
  summarise(Unclean_Boardings = sum(Boarding),
            Unclean_Alightings = sum(Alighting),
            Percent_Difference = (1 - sum(Boarding)/ sum(Alighting))) %>%
  formattable::formattable(align = "r")

# check to insure no zero boarding vehicles

Apc_BYD %>%
  filter(MDCID >= 1950, MDCID < 2000, Transit_Day > "2019-09-20") %>%
  group_by(MDCID, Transit_Day) %>%
  summarise(Unclean_Boardings = sum(Boarding),
            Unclean_Alightings = sum(Alighting),
            Percent_Difference = (1 - sum(Boarding)/ sum(Alighting))) %>%
  formattable::formattable(align = "r")

# get GFI ridership for 1993 on 9/21 and 9/22

# (no data)

####

# table Apc_data does not have block infromation.
# have to go to FactFare

# Wait! it does have block information!

Apc_BYD %>%
  filter(MDCID >= 1950, MDCID < 2000, Transit_Day > "2019-09-20") %>%
  group_by(Transit_Day, MDCID, Block) %>%
  summarise(Boardings = sum(Boarding)) %>%
  select(Date = Transit_Day, Vehicle = MDCID, everything()) %>%
  formattable::formattable(align = "r")

Apc_BYD %>%
  filter(MDCID >= 1950, MDCID < 2000, Transit_Day > "2019-09-20") %>%
  group_by(Transit_Day, MDCID) %>%
  summarise(Boardings = sum(Boarding)) %>%
  select(Date = Transit_Day, Vehicle = MDCID, everything()) %>%
  formattable::formattable(align = "r")

Apc_BYD %>%
  filter(MDCID >= 1950, MDCID < 2000, Transit_Day > "2019-09-20") %>%
  group_by(Transit_Day, Block) %>%
  summarise(Boardings = sum(Boarding)) %>%
  select(Date = Transit_Day, everything()) %>%
  formattable::formattable(align = "r")

# data for Annette request 9/23


con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_Data_90_historical <- tbl(con, "Apc_Data") %>%
  filter(MDCID >= 1950, MDCID < 2000, Route == 90,
         GPS_Time >= "2019-09-01") %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_Data_90_historical$Date <- as.Date(str_sub(Apc_Data_90_historical$GPS_Time, 1, 10))

Apc_Data_90_historical$Clock_Time <- str_sub(Apc_Data_90_historical$GPS_Time, 12, 19)

Apc_Data_90_historical$DateTest <- ifelse(Apc_Data_90_historical$Clock_Time < 
                                            "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_Data_90_historical$Transit_Day_Unix <- ifelse(Apc_Data_90_historical$DateTest == 1,
                                                  lubridate::as_date(Apc_Data_90_historical$Date - 1),
                                                  Apc_Data_90_historical$Date)

# add two dates together

Apc_Data_90_historical$Epoch_Date <- as.Date("1970-01-01")

Apc_Data_90_historical$Transit_Day <- Apc_Data_90_historical$Epoch_Date + lubridate::days(Apc_Data_90_historical$Transit_Day_Unix)

# inspect ridership per trip per day...

Apc_Data_90_historical %>%
  filter(Transit_Day >= "2019-09-01") %>%
  group_by(Transit_Day, Trip) %>%
  summarise(Boardings = sum(Boarding)) %>%
  View()

# get number of trips

trips_total <- Apc_Data_90_historical %>%
  filter(Transit_Day >= "2019-09-01") %>%
  group_by(Transit_Day, Trip) %>%
  distinct(Transit_Day, Trip) %>%
  count() %>%
  nrow() # ... must be better way. 

# get number of trips with no boarding data

trips_no_apc_data <- Apc_Data_90_historical %>%
  filter(Transit_Day >= "2019-09-01") %>%
  group_by(Transit_Day, Trip) %>%
  summarise(Boardings = sum(Boarding)) %>%
  filter(Boardings == 0) %>%
  group_by(Transit_Day, Trip) %>%
  distinct(Transit_Day, Trip) %>%
  count() %>%
  nrow()

trips_no_apc_data / trips_total

# let's get average ridership per trip.

Apc_Data_90_historical %>%
  filter(Transit_Day >= "2019-09-01") %>%
  summarise(Boardings = sum(Boarding) / (trips_total - trips_no_apc_data)) 

# then we would assign that ridership to trips with zero boardings....

#### other methods

# why don't we just double check those zero boardings trips..

trips_with_no_apc_data <- Apc_Data_90_historical %>%
  filter(Transit_Day >= "2019-09-01") %>%
  group_by(Transit_Day, Trip) %>%
  summarise(Boardings = sum(Boarding)) %>%
  filter(Boardings == 0) %>%
  select(-Boardings)

trips_with_no_apc_data_raw <- Apc_Data_90_historical %>%
  inner_join(trips_with_no_apc_data,
             by = c("Transit_Day", "Trip")) 

# now let's get vehicle/day combination and compare with GFI

distinct(trips_with_no_apc_data_raw, Transit_Day, MDCID) %>%
  View()

## SO THIS IS INTERESTING ###

# there are vehicles on here that are not on weekly exception report...

# for example, 1985 and 1988 on the 21st...

# let's look at 1988 on the 21st... maybe that one trip was just a fluke?

Apc_Data_90_historical %>%
  filter(Transit_Day == "2019-09-21", MDCID == 1988) %>%
  left_join(select(filter(DimStop, is.na(DeleteDate)), StopDesc, StopID), by = c("Stop" = "StopID"))%>% 
  View() # not so good, b/c Stop is duplicative...

# so 1988 may be a fluke.... one of its trips was VERY late-night (like 1 am to 2 am)

###

# So now let's get boardings by transit day

Apc_Data_90_historical %>%
  filter(MDCID >= 1950, MDCID < 2000) %>%
  group_by(Transit_Day) %>%
  summarise(Unclean_Boardings = sum(Boarding),
            Unclean_Alightings = sum(Alighting),
            Percent_Difference = (1 - sum(Boarding)/ sum(Alighting))) %>%
  formattable::formattable(align = "r")

# check to insure no zero boarding vehicles

Apc_Data_90_historical %>%
  filter(MDCID >= 1950, MDCID < 2000, Transit_Day > "2019-09-20") %>%
  group_by(MDCID, Transit_Day) %>%
  summarise(Unclean_Boardings = sum(Boarding),
            Unclean_Alightings = sum(Alighting),
            Percent_Difference = (1 - sum(Boarding)/ sum(Alighting))) %>%
  formattable::formattable(align = "r")

# get GFI ridership for 1993 on 9/21 and 9/22

# (no data)

####

# table Apc_data does not have block infromation.
# have to go to FactFare

# Wait! it does have block information!

Apc_Data_90_historical %>%
  filter(MDCID >= 1950, MDCID < 2000, Transit_Day > "2019-09-20") %>%
  group_by(Transit_Day, MDCID, Block) %>%
  summarise(Boardings = sum(Boarding)) %>%
  select(Date = Transit_Day, Vehicle = MDCID, everything()) %>%
  formattable::formattable(align = "r")

Apc_Data_90_historical %>%
  filter(MDCID >= 1950, MDCID < 2000, Transit_Day > "2019-09-20") %>%
  group_by(Transit_Day, MDCID) %>%
  summarise(Boardings = sum(Boarding)) %>%
  select(Date = Transit_Day, Vehicle = MDCID, everything()) %>%
  formattable::formattable(align = "r")

Apc_Data_90_historical %>%
  filter(MDCID >= 1950, MDCID < 2000, Transit_Day > "2019-09-20") %>%
  group_by(Transit_Day, Block) %>%
  summarise(Boardings = sum(Boarding)) %>%
  select(Date = Transit_Day, everything()) %>%
  formattable::formattable(align = "r")

####################

# 9/26/19 RG request


con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Ryan_data <- tbl(con, "Apc_Data") %>%
  filter(Route %in% c(19, 39), Stop == 51362,
         GPS_Time >= "2019-08-31", GPS_Time <= "2019-09-26") %>%
  collect()

# clean dates


Ryan_d                                                 ata$Date <- as.Date(str_sub(Ryan_data$GPS_Time, 1, 10))

Ryan_data$Clock_Time <- str_sub(Ryan_data$GPS_Time, 12, 19)

Ryan_data$DateTest <- ifelse(Ryan_data$Clock_Time < 
                             "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Ryan_data$Transit_Day_Unix <- ifelse(Ryan_data$DateTest == 1,
                                   lubridate::as_date(Ryan_data$Date - 1),
                                   Ryan_data$Date)

# add two dates together

Ryan_data$Epoch_Date <- as.Date("1970-01-01")

Ryan_data$Transit_Day <- Ryan_data$Epoch_Date + lubridate::days(Ryan_data$Transit_Day_Unix)

# extract weekdays

Ryan_data$Day_of_week <- lubridate::wday(Ryan_data$Transit_Day, label = TRUE)

# get only weekdays, labor day

Ryan_data_clean <- Ryan_data %>%
  filter(!Day_of_week %in% c("Sun", "Sat")) %>%
  filter(!Transit_Day == "2019-09-02")

# transform

Ryan_data_transformed <- Ryan_data_clean %>%
  group_by(Transit_Day, Route) %>%
  summarise(Boardings = sum(Boarding),
                Alightings = sum(Alighting))

View(Ryan_data_transformed)

Ryan_data_transformed %>%
  group_by(Route) %>%
  summarise(Boardings = sum(Boardings),
            Alightings = sum(Alightings))

Ryan_data_transformed %>%
  group_by(Route) %>%
  summarise(Avg_Boardings = sum(Boardings)/ n(),
            Avg_Alightings = sum(Alightings)/ n())

######### 10/1/19 Update #############

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_Data_problem_BYD <- tbl(con, "Apc_Data") %>%
  filter(MDCID == 1986 | MDCID == 1988, Route == 90,
         GPS_Time >= "2019-09-24") %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_Data_problem_BYD$Date <- as.Date(str_sub(Apc_Data_problem_BYD$GPS_Time, 1, 10))

Apc_Data_problem_BYD$Clock_Time <- str_sub(Apc_Data_problem_BYD$GPS_Time, 12, 19)

Apc_Data_problem_BYD$DateTest <- ifelse(Apc_Data_problem_BYD$Clock_Time < 
                                            "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_Data_problem_BYD$Transit_Day_Unix <- ifelse(Apc_Data_problem_BYD$DateTest == 1,
                                                  lubridate::as_date(Apc_Data_problem_BYD$Date - 1),
                                                  Apc_Data_problem_BYD$Date)

# add two dates together

Apc_Data_problem_BYD$Epoch_Date <- as.Date("1970-01-01")

Apc_Data_problem_BYD$Transit_Day <- Apc_Data_problem_BYD$Epoch_Date + lubridate::days(Apc_Data_problem_BYD$Transit_Day_Unix)

# inspect ridership per trip per day...

Apc_Data_problem_BYD %>%
  filter(Transit_Day >= "2019-09-01") %>%
  group_by(Transit_Day, MDCID) %>%
  summarise(Boardings = sum(Boarding),
            Alightings= sum(Alighting)) %>%
  arrange(MDCID, Transit_Day) %>%
  View() # yeah looks like a problems... what do data look like?

##### 10/2/19 ##### 

# get and validate data on 1986 and 1988 for Brandon

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_Data_problem_BYD <- tbl(con, "Apc_Data") %>%
  filter(MDCID == 1986 | MDCID == 1988, Route == 90,
         GPS_Time >= "2019-09-24") %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_Data_problem_BYD$Date <- as.Date(str_sub(Apc_Data_problem_BYD$GPS_Time, 1, 10))

Apc_Data_problem_BYD$Clock_Time <- str_sub(Apc_Data_problem_BYD$GPS_Time, 12, 19)

Apc_Data_problem_BYD$DateTest <- ifelse(Apc_Data_problem_BYD$Clock_Time < 
                                          "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_Data_problem_BYD$Transit_Day_Unix <- ifelse(Apc_Data_problem_BYD$DateTest == 1,
                                                lubridate::as_date(Apc_Data_problem_BYD$Date - 1),
                                                Apc_Data_problem_BYD$Date)

# add two dates together

Apc_Data_problem_BYD$Epoch_Date <- as.Date("1970-01-01")

Apc_Data_problem_BYD$Transit_Day <- Apc_Data_problem_BYD$Epoch_Date + lubridate::days(Apc_Data_problem_BYD$Transit_Day_Unix)

# inspect ridership per trip per day...

Apc_Data_problem_BYD %>%
  filter(Transit_Day >= "2019-09-01") %>%
  group_by(Transit_Day, MDCID) %>%
  summarise(Boardings = sum(Boarding),
            Alightings= sum(Alighting)) %>%
  arrange(MDCID, Transit_Day) %>%
  View()

# need to validate those data....

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

Vehicle_Message_History_raw_sample_90 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-09-25 03:00:00", Route == 90) %>%
  collect() 

# add Transit Date to VMH

Vehicle_Message_History_raw_sample_90$Date <- as.Date(str_sub(Vehicle_Message_History_raw_sample_90$Time, 1, 10))

Vehicle_Message_History_raw_sample_90$Clock_Time <- str_sub(Vehicle_Message_History_raw_sample_90$Time, 12, 19)

Vehicle_Message_History_raw_sample_90$DateTest <- ifelse(Vehicle_Message_History_raw_sample_90$Clock_Time < 
                                                                  "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_raw_sample_90$Transit_Day_Unix <- ifelse(Vehicle_Message_History_raw_sample_90$DateTest == 1,
                                                                        lubridate::as_date(Vehicle_Message_History_raw_sample_90$Date - 1),
                                                                        Vehicle_Message_History_raw_sample_90$Date)

# add two dates together

Vehicle_Message_History_raw_sample_90$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_raw_sample_90$Transit_Day <- Vehicle_Message_History_raw_sample_90$Epoch_Date + lubridate::days(Vehicle_Message_History_raw_sample_90$Transit_Day_Unix)

# get boardings per vehicle/trip

problem_vehicle_boardings <- Vehicle_Message_History_raw_sample_90 %>%
  filter(Transit_Day >= "2019-09-24") %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights))
  
problem_vehicle_boardings %>%
  filter(Vehicle_ID %in% c(1986, 1988))

# so.... still bad, but can't validate data right now :(

######## 10/3/19 Update ##########


con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_Route_90 <- tbl(con, "Apc_Data") %>%
  filter(Route == 90, GPS_Time >= "2019-09-01") %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_Route_90$Date <- as.Date(str_sub(Apc_Route_90$GPS_Time, 1, 10))

Apc_Route_90$Clock_Time <- str_sub(Apc_Route_90$GPS_Time, 12, 19)

Apc_Route_90$DateTest <- ifelse(Apc_Route_90$Clock_Time < 
                                      "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_Route_90$Transit_Day_Unix <- ifelse(Apc_Route_90$DateTest == 1,
                                            lubridate::as_date(Apc_Route_90$Date - 1),
                                            Apc_Route_90$Date)

# add two dates together

Apc_Route_90$Epoch_Date <- as.Date("1970-01-01")

Apc_Route_90$Transit_Day <- Apc_Route_90$Epoch_Date + lubridate::days(Apc_Route_90$Transit_Day_Unix)

# So let's pull out Red Line service

Apc_Route_90_Local <- Apc_Route_90 %>%
  group_by(MDCID, Transit_Day, Trip) %>%
  filter(any(str_which(Stop, '^5|^3|14124') ))

# inspect

View(Apc_Route_90_Local) # looks good

sort(unique(Apc_Route_90_Local$Stop))

# get list of date, block, vehicle

Apc_Route_90_Local %>%
  ungroup() %>%
  filter(Transit_Day < "2019-10-01") %>%
  distinct(Transit_Day, MDCID, Block) %>%
  arrange(Transit_Day, MDCID) %>%
  View()

# export

Apc_Route_90_Local %>%
  ungroup() %>%
  filter(Transit_Day < "2019-10-01") %>%
  distinct(Transit_Day, MDCID, Block) %>%
  arrange(Transit_Day, MDCID) %>%
  formattable::formattable(align = "r")



# review data to confirm this worked

View(Apc_Red_Line)

Apc_Red_Line %$%
  sort(unique(Stop))

# let's get stops, join, and then inspect those trips with non-BRT station stops

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                       Database = "DW_IndyGo", 
                       Port = 1433)

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

DimStop <- DimStop_raw %>%
  filter(is.na(DeleteDate), !is.na(Latitude), ActiveInd == 1) # MUST BE ACTIVE

Apc_Red_Line_joined <- Apc_Red_Line %>%
  left_join(select(DimStop, StopID, StopDesc), by = c("Stop" = "StopID"))

# confirm join OK

nrow(Apc_Red_Line)
nrow(Apc_Red_Line_joined) 

# revise bad vehicle list

bad_vehicles <- Apc_BYD_Route_90 %>%
  group_by(MDCID, Transit_Day, Trip) %>%
  filter(any(str_which(Stop, '^5') )) %>%
  filter(any(str_which(Stop, '^3') )) %>%
  filter(any(str_which(Stop, '14124') ))

Apc_Red_Line <-  Apc_BYD_Route_90 %>%
  anti_join(bad_vehicles)

Apc_Red_Line_joined <- Apc_Red_Line %>%
  left_join(select(DimStop, StopID, StopDesc), by = c("Stop" = "StopID"))

######## 11/5/19 Update ##########

library(magrittr)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_Route_90 <- tbl(con, "Apc_Data") %>%
  filter(Route == 90, GPS_Time >= "2019-09-30") %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_Route_90$Date <- as.Date(str_sub(Apc_Route_90$GPS_Time, 1, 10))

Apc_Route_90$Clock_Time <- str_sub(Apc_Route_90$GPS_Time, 12, 19)

Apc_Route_90$DateTest <- ifelse(Apc_Route_90$Clock_Time < 
                                  "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_Route_90$Transit_Day_Unix <- ifelse(Apc_Route_90$DateTest == 1,
                                        lubridate::as_date(Apc_Route_90$Date - 1),
                                        Apc_Route_90$Date)

# add two dates together

Apc_Route_90$Epoch_Date <- as.Date("1970-01-01")

Apc_Route_90$Transit_Day <- Apc_Route_90$Epoch_Date + lubridate::days(Apc_Route_90$Transit_Day_Unix)

# So let's pull out Red Line service

Apc_Route_90_Local <- Apc_Route_90 %>%
  group_by(MDCID, Transit_Day, Trip) %>%
  filter(any(str_which(Stop, '^5|^3|14124') ))

# inspect

View(Apc_Route_90_Local) # looks good

sort(unique(Apc_Route_90_Local$Stop))

# get list of date, block, vehicle

Apc_Route_90_Local %>%
  ungroup() %>%
  filter(Transit_Day < "2019-11-01", Transit_Day >= "2019-10-01") %>%
  distinct(Transit_Day, MDCID, Block) %>%
  arrange(Transit_Day, MDCID) %>%
  View()

# export

Apc_Route_90_Local %>%
  ungroup() %>%
  filter(Transit_Day < "2019-11-01", Transit_Day >= "2019-10-01") %>%
  distinct(Transit_Day, MDCID, Block) %>%
  arrange(Transit_Day, MDCID) %>%
  formattable::formattable(align = "r")

# let's also look at FactFare just to confirm that we don't have farebox ridership by stop.

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW",
                       Database = "DW_IndyGo", Port = 1433)

DimDate_sample <- tbl(con2, "DimDate") %>%
  filter(CalendarDate >= "2019-10-01", CalendarDate < "2019-11-01") %>%
  collect()

DimFare_sample <- tbl(con2, "DimFare") %>% # remove APC, test records
  filter(!FareKey %in% c(1001, 1002, 1003, 1004, 1005, 1008, 1010, 1029)) %>%
  collect()

DimRoute_90 <- tbl(con2, "DimRoute") %>%
  filter(RouteReportLabel == "90") %>%
  collect()

FactFare_Route_90 <- tbl(con2, "FactFare") %>%
  filter(DateKey %in% !!DimDate_sample$DateKey,
         RouteKey %in% !!DimRoute_90$RouteKey,
         !FareKey %in% c(1001, 1002, 1003, 1004, 1005, 1008, 1010, 1029)) %>%
  collect()

# review

View(FactFare_Route_90 )

sort(unique(FactFare_Route_90$FareKey))

FactFare_Route_90 %>%
  left_join(DimFare_sample, by = "FareKey") %>%
  filter(FareKey != 1000, StopKey != 1000) %>%
  arrange(DateKey, VehicleKey, TimeKey) %>%
  View() # wow... no records.... 

# the above confirms that farebox records are not being geo-located...

#### 11/12/19 Update ####

# Need ridership just for this past Sunday...

# How would we do that? Best way would probably be to JUST get Sunday data,
# and then back-out expansion factor....

# quick-and-dirty method would be to just revmoe a couple vehicles and then get counts...

# why don't we get data and see how bad it is...


con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_RL_11_10 <- tbl(con, "Apc_Data") %>%
  filter(MDCID >= 1950, MDCID < 2000, 
         GPS_Time >= "2019-11-09", GPS_Time <= "2019-11-12",
         Route == "90") %>%
  collect()

Apc_RL_11_10$Date <- as.Date(str_sub(Apc_RL_11_10$GPS_Time, 1, 10))

Apc_RL_11_10$Clock_Time <- str_sub(Apc_RL_11_10$GPS_Time, 12, 19)

Apc_RL_11_10$DateTest <- ifelse(Apc_RL_11_10$Clock_Time < 
                                            "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_RL_11_10$Transit_Day_Unix <- ifelse(Apc_RL_11_10$DateTest == 1,
                                                  lubridate::as_date(Apc_RL_11_10$Date - 1),
                                                  Apc_RL_11_10$Date)

# add two dates together

Apc_RL_11_10$Epoch_Date <- as.Date("1970-01-01")

Apc_RL_11_10$Transit_Day <- Apc_RL_11_10$Epoch_Date + lubridate::days(Apc_RL_11_10$Transit_Day_Unix)

# inspect ridership per trip per day...

Apc_RL_11_10 %>%
  filter(Transit_Day == "2019-11-10") %>%
  group_by(Transit_Day, MDCID) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Difference = sum(Boarding) - sum(Alighting)) %>%
  View()

# OK! we look pretty good. No wildly innaccurate boardings, and no guilty vehicles..

Apc_RL_11_10 <- Apc_RL_11_10 %>%
  filter(Transit_Day == "2019-11-10")

# now we need to break it out by time (ie, between 4 and 10 pm..)

Apc_RL_11_10 %>%
  filter(GPS_Time >= "2019-11-10 16:00:00",
         GPS_Time <= "2019-11-10 22:00:00") %>%
  View() # this doesn't look right... ughhhhhhh

Apc_RL_11_10 %>%
  filter(Clock_Time >= "16:00:00",
         Clock_Time <= "22:00:00") %>%
  View()# wow! this works tho lol

Apc_RL_11_10 %>%
  filter(Clock_Time >= "16:00:00",
         Clock_Time <= "22:00:00") %$%
  sum(Boarding)

######### 11/15/19 Update #######

# Get end-of-line ridership numbers since 10/27 for Aaron Vogel.

# Maybe add time of day, too.

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_RL_EOL <- tbl(con, "Apc_Data") %>%
  filter(MDCID >= 1950, MDCID < 2000 | MDCID == 1899, 
         GPS_Time >= "2019-10-26", 
         Route == "90") %>%
  collect()

# get stops

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                       Database = "DW_IndyGo", 
                       Port = 1433)

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

DimStop <- DimStop_raw %>%
  filter(is.na(DeleteDate), !is.na(Latitude), ActiveInd == 1)

# set dates

Apc_RL_EOL$Date <- as.Date(str_sub(Apc_RL_EOL$GPS_Time, 1, 10))

Apc_RL_EOL$Clock_Time <- str_sub(Apc_RL_EOL$GPS_Time, 12, 19)

Apc_RL_EOL$DateTest <- ifelse(Apc_RL_EOL$Clock_Time < 
                                  "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_RL_EOL$Transit_Day_Unix <- ifelse(Apc_RL_EOL$DateTest == 1,
                                        lubridate::as_date(Apc_RL_EOL$Date - 1),
                                        Apc_RL_EOL$Date)

# add two dates together

Apc_RL_EOL$Epoch_Date <- as.Date("1970-01-01")

Apc_RL_EOL$Transit_Day <- Apc_RL_EOL$Epoch_Date + lubridate::days(Apc_RL_EOL$Transit_Day_Unix)

# add adhoc trip number

Apc_RL_EOL$Transit_Day_for_mutate <- str_replace_all(Apc_RL_EOL$Transit_Day,"-", "")

Apc_RL_EOL$AdHocUniqueTripNumber <- str_c(Apc_RL_EOL$Transit_Day_for_mutate, 
                                          Apc_RL_EOL$Trip,
                                          Apc_RL_EOL$MDCID,
                                          Apc_RL_EOL$Run)

# apply quick validation test.

Apc_RL_EOL %>%
  group_by(Transit_Day, MDCID) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Difference = sum(Boarding) - sum(Alighting)) %>%
  View()

# remove bad vehicles (1988 and 1994)

Apc_RL_EOL <- Apc_RL_EOL %>%
  filter(MDCID != 1988, MDCID != 1994)

# join stops

nrow(Apc_RL_EOL)

Apc_RL_EOL_joined <- Apc_RL_EOL %>%
  left_join(DimStop, by = c("Stop" = "StopID"))

nrow(Apc_RL_EOL_joined) == nrow(Apc_RL_EOL) # looks good!

# filter for EOL stops

Apc_RL_EOL_joined_EOL_only <- Apc_RL_EOL_joined %>%
  filter(StopDesc == "96th St & College Ave" | StopDesc == "Greenwood Mall Park & Ride")

# how many trips?

Apc_RL_EOL_joined_EOL_only %>%
  group_by(StopDesc) %>%
  summarise(n_distinct(AdHocUniqueTripNumber)) # NOTE: would this be doubling the number of trips?

# how many boardings?

Apc_RL_EOL_joined_EOL_only %>%
  group_by(StopDesc) %>%
  summarise(Boardings_per_trip = sum(Boarding))

# how many boardings/trips per day?

Apc_RL_EOL_joined_EOL_only %>%
  group_by(StopDesc, Transit_Day) %>%
  summarise(Boardings_per_trip = sum(Boarding),
            Trips = n_distinct(AdHocUniqueTripNumber),
    Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber))

# what is ridership per trip?

Apc_RL_EOL_joined_EOL_only %>%
  group_by(StopDesc) %>%
  summarise(Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber))

# trips per hour of day?

Apc_RL_EOL_joined_EOL_only$Hour_of_Day <- str_sub(Apc_RL_EOL_joined_EOL_only$Clock_Time, 1, 2)

Apc_RL_EOL_joined_EOL_only %>%
  group_by(StopDesc, Hour_of_Day) %>%
  summarise(Trips = n_distinct(AdHocUniqueTripNumber),
            Boardings = sum(Boarding),
            Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber)) %>%
  View()
  
# is there an issue with adhoctripnumber? let's just try date, MDCID, and trip

Apc_RL_EOL_joined_EOL_only %>%
  arrange(Transit_Day, MDCID, GPS_Time) %>%
  View()
 
# yeah.. this will overrepresent trips. 
# let's see if there is another variable that will work.

sort(unique(Apc_RL_EOL_joined_EOL_only$Schedule_Status))

# yeah.......... not sure.
# we may want a different dataset... like *gulp* FactFare...
# or... maybe... do couple more checks.

Apc_RL_EOL_joined_EOL_only %>%
  group_by(Schedule_Status) %>%
  summarise(Boards = sum(Boarding),
            n = n())

# So it looks like I could remove those trip starts...

Apc_RL_EOL_joined_EOL_only_clean <- Apc_RL_EOL_joined_EOL_only %>%
  filter(Schedule_Status != 17)

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(Schedule_Status) %>%
  summarise(Boards = sum(Boarding),
            n = n())

# now re-inspect

Apc_RL_EOL_joined_EOL_only_clean %>%
  arrange(Transit_Day, MDCID, Clock_Time) %>%
  View()

# this looks A LOT better. I think I can work with this.

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc) %>%
  summarise(n_distinct(AdHocUniqueTripNumber)) 

# how many boardings?

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc) %>%
  summarise(Boardings = sum(Boarding))

# how many boardings/trips per day?

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc, Transit_Day) %>%
  summarise(Boardings_per_trip = sum(Boarding),
            Trips = n_distinct(AdHocUniqueTripNumber),
            Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber))

# what is ridership per trip?

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc) %>%
  summarise(Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber))

# trips per hour of day?

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc, Hour_of_Day) %>%
  summarise(Trips = n_distinct(AdHocUniqueTripNumber),
            Boardings = sum(Boarding),
            Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber)) %>%
  View()

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc, Hour_of_Day) %>%
  summarise(Trips = n_distinct(AdHocUniqueTripNumber),
            Boardings = sum(Boarding),
            Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber)) %>%
  ggplot()+
  geom_density(aes(x = Boardings))+
  facet_wrap(~StopDesc)

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc, Hour_of_Day) %>%
  summarise(Trips = n_distinct(AdHocUniqueTripNumber),
            Boardings = sum(Boarding),
            Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber)) %>%
  ggplot()+
  geom_bar(aes(x = Hour_of_Day, y = Boardings), stat = "identity")+
  facet_wrap(~StopDesc)

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc, Hour_of_Day) %>%
  summarise(Trips = n_distinct(AdHocUniqueTripNumber),
            Boardings = sum(Boarding),
            Boardings_per_trip = sum(Boarding) / n_distinct(AdHocUniqueTripNumber)) %>%
  ggplot()+
  geom_bar(aes(x = Hour_of_Day, y = Boardings_per_trip), stat = "identity")+
  facet_wrap(~StopDesc)

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc) %>%
  summarise(Average_Board = mean(Boarding))

# let's review the schedule. According to the schedule, these are the number of trips:

# Greenwood: 55 on weekdays

# 96: 

 # (Update..... I had the number of trips wrong when looking at schedule lol. I think OG data is right.)

# check greenwood.

Apc_RL_EOL_joined_EOL_only_clean %>%
  group_by(StopDesc, Transit_Day) %>%
  summarise(Trips = n_distinct(AdHocUniqueTripNumber)) %>%
  View()

# waaaay too many. I think some of these may be true EOL or EOT trips.........
# we may need to go back to FactFare...

# let's get factsegment...

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                       Database = "DW_IndyGo", 
                       Port = 1433)

### --- Data Import --- ###

## Get some Fact tables re: boardings to see if they are being populated.

# get vehicles

DimVehicle_RL <- tbl(con2, dbplyr::in_schema("dbo", "DimVehicle")) %>%
  filter(MDCID >= 1950, MDCID < 2000 | MDCID == 1899) %>%
  collect()

# get routes

DimRoute_90 <- tbl(con2, "DimRoute") %>%
  filter(RouteReportLabel == "90") %>%
  collect()

# get sample of dates, first

sample_dates <- tbl(con2, "DimDate") %>%
  filter(CalendarDate >= "2019-10-26") %>%
  select(DateKey, CalendarDate) %>%
  collect()

# try FactSegment

FactSegmentActual_sample <- tbl(con2, "FactSegmentActual") %>%
  filter(DateKey %in% !!sample_dates$DateKey,
         VehicleKey %in% !!DimVehicle_RL$VehicleKey) %>%
  collect()

# join stops

FactSegmentActual_sample_EOL <- FactSegmentActual_sample %>%
  left_join(DimStop, by = c("DepartStopKey" = "StopKey")) %>%
  filter(StopDesc == "96th St & College Ave" | StopDesc == "Greenwood Mall Park & Ride")

# inspect

View(FactSegmentActual_sample_EOL)

# this is trash. boardings and alightings are mixed. DepartStop is equal to ArriveStop. Bleh. I hate this.

# try FactFare

# get dim fare first

DimFare_Boarding <- tbl(con2, "DimFare") %>% # remove APC, test records
  filter(FareKey == c(1001)) %>%
  collect()

# now get factfare

FactFare_sample <- tbl(con2, "FactFare") %>%
  filter(DateKey %in% !!sample_dates$DateKey,
         VehicleKey %in% !!DimVehicle_RL$VehicleKey,
         FareKey %in% !!DimFare_Boarding$FareKey) %>%
  collect()

# join stops

FactFare_sample_joined <- FactFare_sample %>%
  left_join(DimStop, by ="StopKey")

# clean for EOL

FactFare_sample_joined_EOL <- FactFare_sample_joined  %>%
  filter(StopDesc == "96th St & College Ave" | StopDesc == "Greenwood Mall Park & Ride")

# inspect

View(FactFare_sample_joined_EOL)

# let's hook this up by direction just to confirm...

FactFare_sample_joined_EOL %>%
  group_by(StopDesc, Direction) %>%
  summarise(Boardings = sum(FareCount)) # ok.....

# let's remove those where we have bad/irrelevant data

FactFare_sample_joined_EOL_clean <- FactFare_sample_joined_EOL %>%
  filter(Direction != "D")

FactFare_sample_joined_EOL_clean <- FactFare_sample_joined_EOL_clean %>%
  filter(StopDesc == "96th St & College Ave" & Direction == "S" |
           StopDesc == "Greenwood Mall Park & Ride" & Direction == "N"  )

FactFare_sample_joined_EOL_clean %>%
  group_by(StopDesc, Direction) %>%
  summarise(Boardings = sum(FareCount))
  
# now.... number of trips per day

FactFare_sample_joined_EOL_clean %>%
  group_by(StopDesc, DateKey) %>%
  summarise(Trips = n_distinct(TripKey)) %>%
  View()

FactFare_sample_joined_EOL_clean %>%
  group_by(StopDesc, DateKey) %>%
  summarise(Trips = n_distinct(VehicleKey, TripKey, RunKey)) # this doesn't look right..

# re-inspect

FactFare_sample_joined_EOL_clean %>%
  arrange(DateKey, VehicleKey, TimeKey) %>%
  View()

# wow... I don't get this..
# this still does not look right...

FactFare_sample_joined_EOL_clean %>%
  arrange(StopDesc, ServiceDateTime) %>%
  View()

# well... let's get some numbers and see

FactFare_sample_joined_EOL_clean %>%
  group_by(StopDesc) %>%
  summarise(Boards = sum(FareCount),
            Trips = n_distinct(TripKey),
            Boardings_per_Trip = sum(FareCount)/ n_distinct(TripKey),
            Days = n_distinct(DateKey),
            Trips_per_day = n_distinct(TripKey) / n_distinct(DateKey),
            Boardings_per_day = sum(FareCount) / n_distinct(DateKey))

library(magrittr)

FactFare_sample_joined_EOL_clean %>%
  left_join(sample_dates, by = "DateKey") %$%
  range(CalendarDate)

# let's just work with what we know...

DimDate_raw <- tbl(con2, "DimDate") %>%
  collect()

FactFare_sample_joined_EOL_clean %>%
  left_join(DimDate_raw, by = "DateKey") %>%
  View()

FactFare_sample_joined_EOL_clean %>%
  left_join(DimDate_raw, by = "DateKey") %>%
  group_by(StopDesc, WorkdayType) %>%
  summarise(Boards = sum(FareCount),
            Trips = n_distinct(TripKey) * n_distinct(DateKey),
            Trips_per_Type = n_distinct(TripKey),
            Boardings_per_Trip = sum(FareCount)/ (n_distinct(TripKey) * n_distinct(DateKey)),
            Days = n_distinct(DateKey),
            Trips_per_day = n_distinct(TripKey) / n_distinct(DateKey),
            Boardings_per_day = sum(FareCount) / n_distinct(DateKey)) 

###### 12/3/19 Update #####

### --- Data Import --- ###

Vehicle_Message_History_raw_adhoc <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-09-30 03:00:00", Time < "2019-11-02 03:00:00",
         Route_Name == "90", Route == 90,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_adhoc <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-09-30 03:00:00", Time < "2019-11-02 03:00:00") %>%
  collect() # consider doing some semi_join or something 

# get stops

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_raw_adhoc <- Vehicle_Message_History_raw_adhoc  %>%
  left_join(Vehicle_Avl_History_raw_adhoc, by = "Avl_History_Id")

# Filter by latitude to create Red Line set

Vehicle_Message_History_north_of_91st <- Vehicle_Message_History_raw_adhoc %>%
  filter(Latitude > 39.920274)

# Filter by longitude to remove garage boardings, if any

# Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
#   filter(Longitude < -86.173321)

# format dates in VMH

Vehicle_Message_History_north_of_91st$Date <- as.Date(str_sub(Vehicle_Message_History_north_of_91st$Time.x, 1, 10))

Vehicle_Message_History_north_of_91st$Clock_Time <- str_sub(Vehicle_Message_History_north_of_91st$Time.x, 12, 19)

Vehicle_Message_History_north_of_91st$DateTest <- ifelse(Vehicle_Message_History_north_of_91st$Clock_Time < 
                                                        "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_north_of_91st$Transit_Day_Unix <- ifelse(Vehicle_Message_History_north_of_91st$DateTest == 1,
                                                              lubridate::as_date(Vehicle_Message_History_north_of_91st$Date - 1),
                                                              Vehicle_Message_History_north_of_91st$Date)

# add two dates together

Vehicle_Message_History_north_of_91st$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_north_of_91st$Transit_Day <- Vehicle_Message_History_north_of_91st$Epoch_Date + lubridate::days(Vehicle_Message_History_north_of_91st$Transit_Day_Unix)

# just get September data

Vehicle_Message_History_north_of_91st <- Vehicle_Message_History_north_of_91st %>%
  filter(Transit_Day >= "2019-10-01", Transit_Day < "2019-11-01")

# now set service types

# Vehicle_Message_History_north_of_91st$Date <- as.Date(Vehicle_Message_History_north_of_91st$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_north_of_91st$Year <- lubridate::year(Vehicle_Message_History_north_of_91st$Transit_Day)
Vehicle_Message_History_north_of_91st$Month <- lubridate::month(Vehicle_Message_History_north_of_91st$Transit_Day)
Vehicle_Message_History_north_of_91st$Day <- lubridate::wday(Vehicle_Message_History_north_of_91st$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2019, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2019, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_north_of_91st_weekdays <- Vehicle_Message_History_north_of_91st %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_north_of_91st_saturday <- Vehicle_Message_History_north_of_91st %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_north_of_91st_sunday <- Vehicle_Message_History_north_of_91st %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_north_of_91st_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_north_of_91st_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_north_of_91st_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_north_of_91st <- rbind(Vehicle_Message_History_north_of_91st_weekdays, Vehicle_Message_History_north_of_91st_sunday,
                                            Vehicle_Message_History_north_of_91st_saturday)

# mutate calendar day for identifier:

Vehicle_Message_History_north_of_91st$Transit_Day_for_mutate <- str_replace_all(Vehicle_Message_History_north_of_91st$Transit_Day,"-", "")

# add unique trip identifier:

Vehicle_Message_History_north_of_91st$AdHocUniqueTripNumber <- str_c(Vehicle_Message_History_north_of_91st$Inbound_Outbound,
                                                                  Vehicle_Message_History_north_of_91st$Transit_Day_for_mutate,
                                                                  Vehicle_Message_History_north_of_91st$Trip,
                                                                  Vehicle_Message_History_north_of_91st$Vehicle_ID,
                                                                  Vehicle_Message_History_north_of_91st$Run_Id)

# now get clock hour

Vehicle_Message_History_north_of_91st$Clock_Hour <- str_sub(Vehicle_Message_History_north_of_91st$Clock_Time, 1, 2)

# get seconds since midnight

Vehicle_Message_History_north_of_91st$seconds_between_dates <- difftime(Vehicle_Message_History_north_of_91st$Date,
                                                                     Vehicle_Message_History_north_of_91st$Transit_Day,
                                                                     units = "secs")

# maybe just need to use GPS_Time format...

Vehicle_Message_History_north_of_91st$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_north_of_91st$Time.x,
                                                                               Vehicle_Message_History_north_of_91st$Date, 
                                                                               units = "secs")

# add those two seconds to get seconds since (true) midnight

Vehicle_Message_History_north_of_91st$seconds_since_midnight <- Vehicle_Message_History_north_of_91st$seconds_since_midnight_GPS_Time +
  Vehicle_Message_History_north_of_91st$seconds_between_dates

# clean the format up (dplyr no like POSIXlt)

Vehicle_Message_History_north_of_91st$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_north_of_91st$seconds_since_midnight))

# take a look at ridership by stop:

Vehicle_Message_History_north_of_91st %>%
  group_by(Stop_Name) %>%
  summarise(Boardings = sum(Boards))

# review potentially bad vehicles (1988, 1993, 1994, currently)

Vehicle_Message_History_north_of_91st %>%
  group_by(Vehicle_ID) %>%
  summarise(Boardings = sum(Boards)) %>%
  arrange(desc(Boardings)) %>%
  View()

# take out 1988, 1993, 1994

Vehicle_Message_History_north_of_91st %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Name) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Boards) / 30) 
  
Vehicle_Message_History_north_of_91st  %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Name) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Boards) / 30) %$%
  sum(Boardings) / 30 

Vehicle_Message_History_north_of_91st %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Name) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Boards) / 30) %$%
  sum(Alightings) / 30 

# side-quest: where are these undefined stops?

Vehicle_Message_History_north_of_91st %>%
  filter(Stop_Name == "Undefined",
         Boards > 0 | Alights > 0) %>%
  leaflet() %>%
  addProviderTiles(providers$Stamen.Toner) %>%
  addCircles(color = "orange", weight = 3, 
              opacity = .9, fillOpacity = 0.5,
              highlightOptions = highlightOptions(color = "white", weight = 2,
                                                  bringToFront = TRUE),
              lat = ~Latitude,
              lng = ~Longitude)

#### 12/4/19 Update #####

### --- Data Import --- ###

Vehicle_Message_History_raw_adhoc <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00",
         Route_Name == "90", Route == 90,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_adhoc <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00") %>%
  collect() # consider doing some semi_join or something 

# get stops

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_raw_adhoc <- Vehicle_Message_History_raw_adhoc  %>%
  left_join(Vehicle_Avl_History_raw_adhoc, by = "Avl_History_Id")

# Filter by GWoodMallPkRide

Vehicle_Message_History_GWMPAR <- Vehicle_Message_History_raw_adhoc %>%
  filter(Stop_Name == "GWoodMallPkRide")

# Filter by longitude to remove garage boardings, if any

# Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
#   filter(Longitude < -86.173321)

# format dates in VMH

Vehicle_Message_History_GWMPAR$Date <- as.Date(str_sub(Vehicle_Message_History_GWMPAR$Time.x, 1, 10))

Vehicle_Message_History_GWMPAR$Clock_Time <- str_sub(Vehicle_Message_History_GWMPAR$Time.x, 12, 19)

Vehicle_Message_History_GWMPAR$DateTest <- ifelse(Vehicle_Message_History_GWMPAR$Clock_Time < 
                                                           "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_GWMPAR$Transit_Day_Unix <- ifelse(Vehicle_Message_History_GWMPAR$DateTest == 1,
                                                                 lubridate::as_date(Vehicle_Message_History_GWMPAR$Date - 1),
                                                                 Vehicle_Message_History_GWMPAR$Date)

# add two dates together

Vehicle_Message_History_GWMPAR$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_GWMPAR$Transit_Day <- Vehicle_Message_History_GWMPAR$Epoch_Date + lubridate::days(Vehicle_Message_History_GWMPAR$Transit_Day_Unix)

# just get October data

Vehicle_Message_History_GWMPAR <- Vehicle_Message_History_GWMPAR %>%
  filter(Transit_Day >= "2019-11-01", Transit_Day < "2019-12-01")

# now set service types

# Vehicle_Message_History_GWMPAR$Date <- as.Date(Vehicle_Message_History_GWMPAR$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_GWMPAR$Year <- lubridate::year(Vehicle_Message_History_GWMPAR$Transit_Day)
Vehicle_Message_History_GWMPAR$Month <- lubridate::month(Vehicle_Message_History_GWMPAR$Transit_Day)
Vehicle_Message_History_GWMPAR$Day <- lubridate::wday(Vehicle_Message_History_GWMPAR$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2019, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2019, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_GWMPAR_weekdays <- Vehicle_Message_History_GWMPAR %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_GWMPAR_saturday <- Vehicle_Message_History_GWMPAR %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_GWMPAR_sunday <- Vehicle_Message_History_GWMPAR %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_GWMPAR_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_GWMPAR_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_GWMPAR_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_GWMPAR <- rbind(Vehicle_Message_History_GWMPAR_weekdays, Vehicle_Message_History_GWMPAR_sunday,
                                               Vehicle_Message_History_GWMPAR_saturday)

# mutate calendar day for identifier:

Vehicle_Message_History_GWMPAR$Transit_Day_for_mutate <- str_replace_all(Vehicle_Message_History_GWMPAR$Transit_Day,"-", "")

# add unique trip identifier:

Vehicle_Message_History_GWMPAR$AdHocUniqueTripNumber <- str_c(Vehicle_Message_History_GWMPAR$Inbound_Outbound,
                                                                     Vehicle_Message_History_GWMPAR$Transit_Day_for_mutate,
                                                                     Vehicle_Message_History_GWMPAR$Trip,
                                                                     Vehicle_Message_History_GWMPAR$Vehicle_ID,
                                                                     Vehicle_Message_History_GWMPAR$Run_Id)

# now get clock hour

Vehicle_Message_History_GWMPAR$Clock_Hour <- str_sub(Vehicle_Message_History_GWMPAR$Clock_Time, 1, 2)

# get seconds since midnight

Vehicle_Message_History_GWMPAR$seconds_between_dates <- difftime(Vehicle_Message_History_GWMPAR$Date,
                                                                        Vehicle_Message_History_GWMPAR$Transit_Day,
                                                                        units = "secs")

# maybe just need to use GPS_Time format...

Vehicle_Message_History_GWMPAR$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_GWMPAR$Time.x,
                                                                                  Vehicle_Message_History_GWMPAR$Date, 
                                                                                  units = "secs")

# add those two seconds to get seconds since (true) midnight

Vehicle_Message_History_GWMPAR$seconds_since_midnight <- Vehicle_Message_History_GWMPAR$seconds_since_midnight_GPS_Time +
  Vehicle_Message_History_GWMPAR$seconds_between_dates

# clean the format up (dplyr no like POSIXlt)

Vehicle_Message_History_GWMPAR$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_GWMPAR$seconds_since_midnight))

# take a look at ridership by stop:

Vehicle_Message_History_GWMPAR %>%
  group_by(Stop_Name) %>%
  summarise(Boardings = sum(Boards))

# review potentially bad vehicles (1988, 1993, 1994, currently)

Vehicle_Message_History_GWMPAR %>%
  group_by(Vehicle_ID) %>%
  summarise(Boardings = sum(Boards)) %>%
  arrange(desc(Boardings)) %>%
  View()

# take out 1988, 1993, 1994

Vehicle_Message_History_GWMPAR %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Name) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Alights) / 30) 

Vehicle_Message_History_GWMPAR  %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Name) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Alights) / 30) %$%
  sum(Boardings) / 30 

Vehicle_Message_History_GWMPAR %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Name) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Alights) / 30) %$%
  sum(Alightings) / 30 

### also look at stop 52952 ###

# Filter by 52952

Vehicle_Message_History_52952 <- Vehicle_Message_History_raw_adhoc %>%
  filter(Stop_Id == "52952")

# Filter by longitude to remove garage boardings, if any

# Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
#   filter(Longitude < -86.173321)

# format dates in VMH

Vehicle_Message_History_52952$Date <- as.Date(str_sub(Vehicle_Message_History_52952$Time.x, 1, 10))

Vehicle_Message_History_52952$Clock_Time <- str_sub(Vehicle_Message_History_52952$Time.x, 12, 19)

Vehicle_Message_History_52952$DateTest <- ifelse(Vehicle_Message_History_52952$Clock_Time < 
                                                    "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_52952$Transit_Day_Unix <- ifelse(Vehicle_Message_History_52952$DateTest == 1,
                                                          lubridate::as_date(Vehicle_Message_History_52952$Date - 1),
                                                          Vehicle_Message_History_52952$Date)

# add two dates together

Vehicle_Message_History_52952$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_52952$Transit_Day <- Vehicle_Message_History_52952$Epoch_Date + lubridate::days(Vehicle_Message_History_52952$Transit_Day_Unix)

# just get September data

Vehicle_Message_History_52952 <- Vehicle_Message_History_52952 %>%
  filter(Transit_Day >= "2019-11-01", Transit_Day < "2019-12-01")

# now set service types

# Vehicle_Message_History_52952$Date <- as.Date(Vehicle_Message_History_52952$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_52952$Year <- lubridate::year(Vehicle_Message_History_52952$Transit_Day)
Vehicle_Message_History_52952$Month <- lubridate::month(Vehicle_Message_History_52952$Transit_Day)
Vehicle_Message_History_52952$Day <- lubridate::wday(Vehicle_Message_History_52952$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2019, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2019, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_52952_weekdays <- Vehicle_Message_History_52952 %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_52952_saturday <- Vehicle_Message_History_52952 %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_52952_sunday <- Vehicle_Message_History_52952 %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_52952_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_52952_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_52952_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_52952 <- rbind(Vehicle_Message_History_52952_weekdays, Vehicle_Message_History_52952_sunday,
                                        Vehicle_Message_History_52952_saturday)

# mutate calendar day for identifier:

Vehicle_Message_History_52952$Transit_Day_for_mutate <- str_replace_all(Vehicle_Message_History_52952$Transit_Day,"-", "")

# add unique trip identifier:

Vehicle_Message_History_52952$AdHocUniqueTripNumber <- str_c(Vehicle_Message_History_52952$Inbound_Outbound,
                                                              Vehicle_Message_History_52952$Transit_Day_for_mutate,
                                                              Vehicle_Message_History_52952$Trip,
                                                              Vehicle_Message_History_52952$Vehicle_ID,
                                                              Vehicle_Message_History_52952$Run_Id)

# now get clock hour

Vehicle_Message_History_52952$Clock_Hour <- str_sub(Vehicle_Message_History_52952$Clock_Time, 1, 2)

# get seconds since midnight

Vehicle_Message_History_52952$seconds_between_dates <- difftime(Vehicle_Message_History_52952$Date,
                                                                 Vehicle_Message_History_52952$Transit_Day,
                                                                 units = "secs")

# maybe just need to use GPS_Time format...

Vehicle_Message_History_52952$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_52952$Time.x,
                                                                           Vehicle_Message_History_52952$Date, 
                                                                           units = "secs")

# add those two seconds to get seconds since (true) midnight

Vehicle_Message_History_52952$seconds_since_midnight <- Vehicle_Message_History_52952$seconds_since_midnight_GPS_Time +
  Vehicle_Message_History_52952$seconds_between_dates

# clean the format up (dplyr no like POSIXlt)

Vehicle_Message_History_52952$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_52952$seconds_since_midnight))

# take a look at ridership by stop:

Vehicle_Message_History_52952 %>%
  group_by(Stop_Id) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights))

# review potentially bad vehicles (1988, 1993, 1994, currently)

Vehicle_Message_History_52952 %>%
  group_by(Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  arrange(desc(Boardings)) %>%
  View()

# take out 1988, 1993, 1994

Vehicle_Message_History_52952 %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Id) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Alights) / 30) 

Vehicle_Message_History_52952  %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Id) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Alights) / 30) %$%
  sum(Boardings) / 30 

Vehicle_Message_History_52952 %>%
  filter(!Vehicle_ID %in% c(1988, 1993, 1994), !is.na(Stop_Name))%>%
  group_by(Stop_Id) %>%
  summarise(Boardings = sum(Boards),
            Average_Daily_Boardings = sum(Boards) / 30,
            Alightings = sum(Alights),
            Average_Daily_Alightings = sum(Alights) / 30) %$%
  sum(Alightings) / 30 

### also get Route 31 at stop 53045 ###

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

Vehicle_Message_History_31_53045 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00",
         Stop_Id == 53045) %>%
  collect() # no results...

Vehicle_Message_History_31_53045 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00",
         Stop_Name == "CntyLn&GWoodMal") %>%
  collect() # no results

Vehicle_Message_History_31 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00",
         Route == 31) %>%
  collect()

sort(unique(Vehicle_Message_History_31$Stop_Id))
sort(unique(Vehicle_Message_History_31$Stop_Name))

Vehicle_Message_History_31 %>%
  filter(Boards > 0 | Alights > 0) %>%
  filter(Stop_Id == 0 | Stop_Name == "Undefinded") %>%
  left_join(Vehicle_Avl_History_raw_adhoc, by = "Avl_History_Id") %>%
  leaflet::leaflet() %>%
  leaflet::addProviderTiles(providers$Stamen.Toner) %>%
  leaflet:: addCircles(color = "orange", weight = 3, 
             opacity = .1, fillOpacity = 0.5,
             highlightOptions = highlightOptions(color = "white", weight = 2,
                                                 bringToFront = TRUE),
             lat = ~Latitude,
             lng = ~Longitude)
  
########### 12/9/19 Request ######

# Get daily boardings at DTC from TMDM, then get from Avail...

### --- Database connection --- ###

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "IPTC-TMDATAMART\\TMDATAMART",
                      Database = "TMDATAMART", Port = 1433)

### --- Data Import --- ###

# get routes

ROUTE_raw <- tbl(con, "ROUTE") %>% 
  select(ROUTE_ID, ROUTE_ABBR) %>%
  collect()

# get stops

GEO_NODE_raw <- tbl(con, "GEO_NODE") %>% 
  select(GEO_NODE_ID, GEO_NODE_ABBR, GEO_NODE_NAME) %>%
  collect()

# get calendar

CALENDAR_raw <- tbl(con, "CALENDAR") %>% 
  select(CALENDAR_ID, CALENDAR_DATE) %>%
  collect()

# get set of GEO_NODE_ID for CTC

GEO_NODE_raw %>%
  filter(str_detect(GEO_NODE_NAME, "CTC")) %>%
  select(GEO_NODE_ID, GEO_NODE_ABBR, GEO_NODE_NAME) %>%
  View() # looks good

GEO_NODE_raw %>%
  filter(str_detect(GEO_NODE_NAME, "CTC")) %>%
  select(GEO_NODE_ID)

# set query

PASSENGER_COUNT_query <- tbl(con, sql("SELECT CALENDAR_ID, ROUTE_ID, GEO_NODE_ID, 
                                      BOARD, ALIGHT, VEHICLE_ID, TRIP_ID
                                      from PASSENGER_COUNT
                                      WHERE (CALENDAR_ID >= 120160101.0) and (CALENDAR_ID <= 120191231.0) and (BOARD > 0)
                                      and PASSENGER_COUNT.GEO_NODE_ID IN (4409,
        4410,
        4411,
        4412,
        4413,
        4414,
        4415,
        4416,
        4417,
        4418,
        4419,
        4420,
        4421,
        4422,
        4423,
        4424,
        4425,
        4426,
        4427)
                                      and PASSENGER_COUNT.TRIP_ID IS NOT NULL
                                      and PASSENGER_COUNT.VEHICLE_ID IN (SELECT dbo.SCHEDULE.VEHICLE_ID
                                      FROM dbo.SCHEDULE with (nolock) WHERE PASSENGER_COUNT.CALENDAR_ID = dbo.SCHEDULE.CALENDAR_ID
                                      AND PASSENGER_COUNT.TIME_TABLE_VERSION_ID=dbo.SCHEDULE.TIME_TABLE_VERSION_ID
                                      AND PASSENGER_COUNT.ROUTE_ID = dbo.SCHEDULE.ROUTE_ID
                                      AND PASSENGER_COUNT.ROUTE_DIRECTION_ID = dbo.SCHEDULE.ROUTE_DIRECTION_ID
                                      AND PASSENGER_COUNT.TRIP_ID = dbo.SCHEDULE.TRIP_ID
                                      AND PASSENGER_COUNT.GEO_NODE_ID = dbo.SCHEDULE.GEO_NODE_ID)
                                      "))

PASSENGER_COUNT_raw <- PASSENGER_COUNT_query %>% collect()

# PASSENGER_COUNT_raw_sample %>%
#   group_by(TRIP_ID) %>%
#   filter(any(str_detect(GEO_NODE_NAME, "DTC"))) %>%
#   arrange(CALENDAR_ID, TRIP_ID, BLOCK_STOP_ORDER) %>%
#   View()

## transformations  ##

# make copy

df <- PASSENGER_COUNT_raw

# get dates right

df$CALENDAR_ID <- substring(df$CALENDAR_ID, 2)

df$New_Date <- lubridate::ymd(df$CALENDAR_ID)

# now set service types

df$Year <- lubridate::year(df$New_Date)
df$Month <- lubridate::month(df$New_Date)
df$Day <- lubridate::wday(df$New_Date, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

df_weekdays <- df %>%
  filter(Day != "Sat" , Day != "Sun",
         !New_Date %in% holidays_sunday_formatted,
         !New_Date %in% holidays_saturday_formatted)

# set saturday service

df_saturday <- df %>%
  filter(Day == "Sat" |
           New_Date %in% holidays_saturday_formatted)

# set Sunday service

df_sunday <- df %>%
  filter(Day == "Sun" |
           New_Date %in% holidays_sunday_formatted)

# mutate service type

df_sunday$Service_Type <- "Sunday"
df_saturday$Service_Type <- "Saturday"
df_weekdays$Service_Type <- "Weekday"

# rebind

df <- rbind(df_weekdays, df_sunday, df_saturday)

# now show boardings per day

TMDM_CTC_by_date <- df %>%
  group_by(New_Date) %>%
  summarise(Boardings = sum(BOARD),
            Alightings = sum(ALIGHT),
            Activity = sum(BOARD) + sum(ALIGHT)) # consider some other span...

df %>% # look at data
  group_by(New_Date) %>%
  summarise(Boardings = sum(BOARD)) %>%
  ggplot(aes(x = New_Date, y = Boardings))+
  geom_bar(stat = "identity") + 
  geom_abline()

# now get Avail data

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW",
                       Database = "DW_IndyGo", Port = 1433)

DimDate_raw <- tbl(con2, "DimDate") %>%
  filter(CalendarDate >= "2018-05-01", CalendarDate < "2019-12-01") %>%
  collect()

DimFare_APC <- tbl(con2, "DimFare") %>% # remove APC, test records
  filter(FareKey %in% c(1001, 1002, 1003)) %>%
  collect()

DimStop_CTC <- tbl(con2, "DimStop") %>%
  filter(StopID %in% c(90001:90019)) %>%
  collect()

FactFare_CTC <- tbl(con2, "FactFare") %>%
  filter(DateKey %in% !!DimDate_raw$DateKey,
         FareKey %in% c(1001, 1002, 1003),
         StopKey %in% !!DimStop_CTC$StopKey) %>%
  collect()

DimStop_raw <- tbl(con2, "DimStop") %>% collect()

DimVehicle_raw <- tbl(con2, "DimVehicle") %>% collect()

DimTrip_raw <- tbl(con2, "DimTrip") %>% collect()

# now transform FactFare to be sensible...

FactFare_CTC_joined <- FactFare_CTC %>%
  left_join(select(DimFare_APC, FareKey, FareReportLabel), by = "FareKey") %>%
  left_join(select(DimStop_raw, StopKey, StopID, StopReportLabel, StopDesc), by = "StopKey") %>%
  left_join(select(DimVehicle_raw, VehicleKey, MDCID), by = "VehicleKey") %>%
  left_join(DimDate_raw, by = "DateKey")

FactFare_CTC_sensible <- FactFare_CTC_joined %>%
  pivot_wider(., id_cols = FareboxRecordID, names_from = FareReportLabel, 
              values_from = FareCount,
              names_sep = "")

FactFare_CTC_sensible_revised <- FactFare_CTC_joined %>%
  filter(FareReportLabel == "On Board") %>%
  left_join(FactFare_CTC_sensible, by = "FareboxRecordID")

# review for bogus vehicles

FactFare_CTC_sensible_revised %>%
  group_by(MDCID, CalendarDate) %>%
  summarise(Boards = sum(Boarding),
            Alights = sum(Alighting),
            Difference = sum(Boarding) - sum(Alighting)) %>%
  arrange(Boards) %>%
  View() # holy smokes! not sure there are ANY BRT vehicles here?

sort(unique(FactFare_CTC_sensible_revised$MDCID)) # hm... so there are, but... something not right..

FactFare_CTC_sensible_revised %>%
  filter(MDCID %in% c(1950:1999)) %>%
  group_by(MDCID, CalendarDate) %>%
  summarise(Boards = sum(Boarding),
            Alights = sum(Alighting),
            Difference = sum(Boarding) - sum(Alighting)) %>%
  arrange(Boards) %>%
  View() # this isn't right.... welll...... maybe... but this is culled dataset, so I wouldn't get good data this way

# do a quick grab of APC data to identify any other bogus vehicles

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_raw <- tbl(con, "Apc_Data") %>%
  filter(Boarding > 0 | Alighting > 0) %>%
  collect()

# add dates so we can get boardings and alightings per vehicle per day

Apc_raw$Date <- as.Date(str_sub(Apc_raw$GPS_Time, 1, 10))

Apc_raw$Clock_Time <- str_sub(Apc_raw$GPS_Time, 12, 19)

Apc_raw$DateTest <- ifelse(Apc_raw$Clock_Time < 
                                  "03:00:00", 1, 0)

Apc_raw$Transit_Day_Unix <- ifelse(Apc_raw$DateTest == 1,
                                        lubridate::as_date(Apc_raw$Date - 1),
                                        Apc_raw$Date)

Apc_raw$Epoch_Date <- as.Date("1970-01-01")

Apc_raw$Transit_Day <- Apc_raw$Epoch_Date + lubridate::days(Apc_raw$Transit_Day_Unix)

# get raw Avail APC by day to identify bogus vehicles

Apc_raw %>%
  group_by(MDCID, Transit_Day) %>%
  summarise(Boardings = sum(Boarding),
            Aligthtings = sum(Alighting)) %>%
  View() # looks like I also need to remove 1986.. review

Apc_raw %>%
  filter(MDCID == 1986) %>%
  group_by(MDCID, Transit_Day) %>%
  summarise(Boardings = sum(Boarding),
            Aligthtings = sum(Alighting)) %>%
  View() # well..... maybe just remove those to days for 86 (9/25, 9/26, 10/10, 10/12)

# get rid of bogus vehicles

FactFare_CTC_sensible_revised <- FactFare_CTC_sensible_revised %>%
  filter(!MDCID %in% c(1988, 1993, 1994)) 

other_vehicles_dates_to_remove <- FactFare_CTC_sensible_revised %>%
  filter(MDCID == 1986, CalendarDateChar %in% c("09/25/2019", "09/26/2019",
                                                "10/10/2019", "09/12/2019"))
  
FactFare_CTC_sensible_revised <-  FactFare_CTC_sensible_revised %>%
  anti_join(other_vehicles_dates_to_remove, by = c("MDCID","CalendarDateChar" ))

# now get boardings by day

Avail_CTC_by_date <- FactFare_CTC_sensible_revised %>%
  mutate(CalendarDate = as.Date(CalendarDate ))%>%
  group_by(CalendarDate) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Activity = sum(Boarding) + sum(Alighting)) 

# now merge Avail and TMDM data

Combined_CTC_by_date <- full_join(TMDM_CTC_by_date, Avail_CTC_by_date,
                                  by = c("New_Date" = "CalendarDate"))

# replace NAs

Combined_CTC_by_date[is.na(Combined_CTC_by_date)] <- 0

# create combo line

Combined_CTC_by_date$Boardings <- Combined_CTC_by_date$Boardings.x + Combined_CTC_by_date$Boardings.y
Combined_CTC_by_date$Alightings <- Combined_CTC_by_date$Alightings.x + Combined_CTC_by_date$Alightings.y
Combined_CTC_by_date$Activity <- Combined_CTC_by_date$Activity.x + Combined_CTC_by_date$Activity.y

# clean up date column

Combined_CTC_by_date <- rename(Combined_CTC_by_date, Date = New_Date)

# add service type.... then graph each one

Combined_CTC_by_date$Year <- lubridate::year(Combined_CTC_by_date$Date)
Combined_CTC_by_date$Month <- lubridate::month(Combined_CTC_by_date$Date)
Combined_CTC_by_date$Day <- lubridate::wday(Combined_CTC_by_date$Date, label=TRUE)
Combined_CTC_by_date$WeekNo <- lubridate::week(Combined_CTC_by_date$Date)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Combined_CTC_by_date_weekdays <- Combined_CTC_by_date %>%
  filter(Day != "Sat" , Day != "Sun",
         !Date %in% holidays_sunday_formatted,
         !Date %in% holidays_saturday_formatted)

# set saturday service

Combined_CTC_by_date_saturday <- Combined_CTC_by_date %>%
  filter(Day == "Sat" |
           Date %in% holidays_saturday_formatted)

# set Sunday service

Combined_CTC_by_date_sunday <- Combined_CTC_by_date %>%
  filter(Day == "Sun" |
           Date %in% holidays_sunday_formatted)

# mutate service type

Combined_CTC_by_date_sunday$Service_Type <- "Sunday"
Combined_CTC_by_date_saturday$Service_Type <- "Saturday"
Combined_CTC_by_date_weekdays$Service_Type <- "Weekday"

# rebind

Combined_CTC_by_date <- rbind(Combined_CTC_by_date_weekdays, Combined_CTC_by_date_sunday, Combined_CTC_by_date_saturday)

# now! let's wrap plots

Combined_CTC_by_date %>% # wrapped by service type
  ggplot()+
  geom_line(aes(x = Date, y = Boardings), stat = "identity")+
  facet_wrap(~Service_Type)

Combined_CTC_by_date %>% # coded by service type (superior to above)
  ggplot()+
  geom_line(aes(x = Date, y = Boardings,
                group = Service_Type, 
                color = Service_Type), stat = "identity")

Combined_CTC_by_date %>% # smoothed by service type
  filter(Boardings > 1000) %>%  # should remove dates with no/bad data...
  ggplot()+
  geom_smooth(aes(x = Date, y = Boardings,
                group = Service_Type, 
                color = Service_Type), method = "loess", span = 0.15) # span is signifcant.

Combined_CTC_by_date %>% # grouped by week, wrapped by year
  filter(Date < "2019-11-26") %>% # no enough data past this date
  group_by(Year, WeekNo) %>%
  summarise(Boardings = sum(Boardings)) %>%
  ggplot()+
  geom_line(aes(x = WeekNo, y = Boardings), stat = "identity")+
  facet_wrap(~Year)

Combined_CTC_by_date %>% # no wrapping, grouping (not verrry useful...)
  ggplot()+
  geom_line(aes(x = Date, y = Boardings), stat = "identity")

Combined_CTC_by_date %>% # group by week
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>%
  summarise(Weekly_Boardings = sum(Boardings)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot()+
  geom_bar(aes(y =Weekly_Boardings, x = week), stat = "identity") 

Combined_CTC_by_date %>% # average daily, per week
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot()+
  geom_bar(aes(y =Average_Daily_Boardings, x = week), stat = "identity") # I like this...

Combined_CTC_by_date %>% # group by week, smoothed
  filter(Boardings > 1000) %>%
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>% # this turns it into factor...
  summarise(Weekly_Boardings = sum(Boardings)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot(aes(y = Weekly_Boardings, x = week))+
  geom_smooth(method = "loess")

Combined_CTC_by_date %>% # average daily, per week, by service type
  filter(Date < "2019-11-26") %>%
  mutate(week = cut(Date, "week"))%>%
  mutate(week = as.Date(week)) %>%
  group_by(week, Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  ggplot()+
  geom_bar(aes(y =Average_Daily_Boardings, x = week), stat = "identity") +
  facet_wrap(~Service_Type)

Combined_CTC_by_date %>% # same as above, but line graph, coded
  filter(Date < "2019-11-26") %>%
  mutate(week = cut(Date, "week"))%>%
  mutate(week = as.Date(week)) %>%
  group_by(week, Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  ggplot()+
  geom_line(aes(y =Average_Daily_Boardings, x = week,
                group = Service_Type, 
                color = Service_Type)) 

# should probably chart increase from same day/week/month last year...

Combined_CTC_by_date %>% # average daily boardings by month, service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  ggplot()+
  geom_line(aes(y =Average_Daily_Boardings, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  labs(title = "Average Daily Boardings per Month", subtitle = "At the CTC since June, 2016",
       x = "Year", y = "Average Daily Boardings")# consider adding month of year...

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  group_by(Service_Type) %>%
  mutate(Last_Year = dplyr::lag(Average_Daily_Boardings, 12, order_by = month)) %>%
  mutate(Change = (Average_Daily_Boardings - Last_Year)/ Last_Year) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(labels = scales::date_format("%b-%Y"))

library(scales)

Combined_CTC_by_date %>% # same as above, but flipped
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  group_by(Service_Type) %>%
  mutate(Last_Year = dplyr::lag(Average_Daily_Boardings, 12, order_by = month)) %>%
  mutate(Change = (Average_Daily_Boardings - Last_Year)/ Last_Year) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(date_breaks = "1 month",
                   labels = scales::date_format("%b-%Y")) +
  coord_flip()

# same as above, but only with change since 2016

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Boardings, 36, order_by = month)) %>%
  mutate(Change = (Average_Daily_Boardings - Three_Years_Ago)/ Three_Years_Ago) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(labels = scales::date_format("%b-%Y"))

# let's get average daily boardings in the five months following system opening,
# and compare to present time-span.

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Boardings, 36, order_by = month)) %>%
  mutate(Change = (Average_Daily_Boardings - Three_Years_Ago)/ Three_Years_Ago) %>%
  View()

# we should probably cut by quarter or something...

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  filter(Date >= "2016-07-01") %>%
  mutate(month = cut(Date, "quarter"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Boardings, 9, order_by = month)) %>%
  mutate(Change = (Average_Daily_Boardings - Three_Years_Ago)/ Three_Years_Ago) %>%
  View()

# NOTE: should probably mutate for "Activity" or something (combo of boardings and alightings...)
# OR.... just pick whichever one is higher...

######### same as above, but for alightings

Combined_CTC_by_date %>% # wrapped by service type
  ggplot()+
  geom_line(aes(x = Date, y = Alightings), stat = "identity")+
  facet_wrap(~Service_Type) # so many alightings!!!

Combined_CTC_by_date %>% # coded by service type (superior to above)
  ggplot()+
  geom_line(aes(x = Date, y = Alightings,
                group = Service_Type, 
                color = Service_Type), stat = "identity")

Combined_CTC_by_date %>% # smoothed by service type
  filter(Alightings > 1000) %>%  # should remove dates with no/bad data...
  ggplot()+
  geom_smooth(aes(x = Date, y = Alightings,
                  group = Service_Type, 
                  color = Service_Type), method = "loess", span = 0.15) # span is signifcant.

Combined_CTC_by_date %>% # grouped by week, wrapped by year
  filter(Date < "2019-11-26") %>% # no enough data past this date
  group_by(Year, WeekNo) %>%
  summarise(Alightings = sum(Alightings)) %>%
  ggplot()+
  geom_line(aes(x = WeekNo, y = Alightings), stat = "identity")+
  facet_wrap(~Year)

Combined_CTC_by_date %>% # no wrapping, grouping (not verrry useful...)
  ggplot()+
  geom_line(aes(x = Date, y = Alightings), stat = "identity")

Combined_CTC_by_date %>% # group by week
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>%
  summarise(Weekly_Alightings = sum(Alightings)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot()+
  geom_bar(aes(y =Weekly_Alightings, x = week), stat = "identity") 

Combined_CTC_by_date %>% # average daily, per week
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot()+
  geom_bar(aes(y =Average_Daily_Alightings, x = week), stat = "identity") # I like this...

Combined_CTC_by_date %>% # group by week, smoothed
  filter(Alightings > 1000) %>%
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>% # this turns it into factor...
  summarise(Weekly_Alightings = sum(Alightings)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot(aes(y = Weekly_Alightings, x = week))+
  geom_smooth(method = "loess")

Combined_CTC_by_date %>% # average daily, per week, by service type
  filter(Date < "2019-11-26") %>%
  mutate(week = cut(Date, "week"))%>%
  mutate(week = as.Date(week)) %>%
  group_by(week, Service_Type)  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  ggplot()+
  geom_bar(aes(y =Average_Daily_Alightings, x = week), stat = "identity") +
  facet_wrap(~Service_Type)

Combined_CTC_by_date %>% # same as above, but line graph, coded
  filter(Date < "2019-11-26") %>%
  mutate(week = cut(Date, "week"))%>%
  mutate(week = as.Date(week)) %>%
  group_by(week, Service_Type)  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  ggplot()+
  geom_line(aes(y =Average_Daily_Alightings, x = week,
                group = Service_Type, 
                color = Service_Type)) 

# should probably chart increase from same day/week/month last year...

Combined_CTC_by_date %>% # average daily Alightings by month, service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  ggplot()+
  geom_line(aes(y =Average_Daily_Alightings, x = month ,
                group = Service_Type, 
                color = Service_Type)) # consider adding month of year...

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  group_by(Service_Type) %>%
  mutate(Last_Year = dplyr::lag(Average_Daily_Alightings, 12, order_by = month)) %>%
  mutate(Change = (Average_Daily_Alightings - Last_Year)/ Last_Year) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(labels = scales::date_format("%b-%Y"))

library(scales)

Combined_CTC_by_date %>% # same as above, but flipped
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  group_by(Service_Type) %>%
  mutate(Last_Year = dplyr::lag(Average_Daily_Alightings, 12, order_by = month)) %>%
  mutate(Change = (Average_Daily_Alightings - Last_Year)/ Last_Year) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(date_breaks = "1 month",
                   labels = scales::date_format("%b-%Y")) +
  coord_flip()

# same as above, but only with change since 2016

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Alightings, 36, order_by = month)) %>%
  mutate(Change = (Average_Daily_Alightings - Three_Years_Ago)/ Three_Years_Ago) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(labels = scales::date_format("%b-%Y"))

# let's get average daily boardings in the five months following system opening,
# and compare to present time-span.

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Alightings, 36, order_by = month)) %>%
  mutate(Change = (Average_Daily_Alightings - Three_Years_Ago)/ Three_Years_Ago) %>%
  View()

# we should probably cut by quarter or something...

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  filter(Date >= "2016-07-01") %>%
  mutate(month = cut(Date, "quarter"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Alightings = mean(Alightings)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Alightings, 9, order_by = month)) %>%
  mutate(Change = (Average_Daily_Alightings - Three_Years_Ago)/ Three_Years_Ago) %>%
  View()


######### same as above, but for alightings + boardings (activity)

Combined_CTC_by_date %>% # wrapped by service type
  ggplot()+
  geom_line(aes(x = Date, y = Activity), stat = "identity")+
  facet_wrap(~Service_Type)

Combined_CTC_by_date %>% # coded by service type (superior to above)
  ggplot()+
  geom_line(aes(x = Date, y = Activity,
                group = Service_Type, 
                color = Service_Type), stat = "identity")

Combined_CTC_by_date %>% # smoothed by service type
  filter(Activity > 1000) %>%  # should remove dates with no/bad data...
  ggplot()+
  geom_smooth(aes(x = Date, y = Activity,
                  group = Service_Type, 
                  color = Service_Type), method = "loess", span = 0.15) # span is signifcant.

Combined_CTC_by_date %>% # grouped by week, wrapped by year
  filter(Date < "2019-11-26") %>% # no enough data past this date
  group_by(Year, WeekNo) %>%
  summarise(Activity = sum(Activity)) %>%
  ggplot()+
  geom_line(aes(x = WeekNo, y = Activity), stat = "identity")+
  facet_wrap(~Year) +
  labs(title = "Total Activity per Week")

Combined_CTC_by_date %>% # no wrapping, grouping (not verrry useful...)
  ggplot()+
  geom_line(aes(x = Date, y = Activity), stat = "identity")

Combined_CTC_by_date %>% # group by week
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>%
  summarise(Weekly_Activity = sum(Activity)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot()+
  geom_bar(aes(y =Weekly_Activity, x = week), stat = "identity")  +
  labs(title = "Total Activity per Week")

Combined_CTC_by_date %>% # average daily, per week
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot()+
  geom_bar(aes(y =Average_Daily_Activity, x = week), stat = "identity") # I like this...

Combined_CTC_by_date %>% # group by week, smoothed
  filter(Activity > 1000) %>%
  filter(Date < "2019-11-26") %>%
  group_by(week = cut(Date, "week"))  %>% # this turns it into factor...
  summarise(Weekly_Activity = sum(Activity)) %>%
  mutate(week = as.Date(week)) %>%
  ggplot(aes(y = Weekly_Activity, x = week))+
  geom_smooth(method = "loess")

Combined_CTC_by_date %>% # average daily, per week, by service type
  filter(Date < "2019-11-26") %>%
  mutate(week = cut(Date, "week"))%>%
  mutate(week = as.Date(week)) %>%
  group_by(week, Service_Type)  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  ggplot()+
  geom_bar(aes(y =Average_Daily_Activity, x = week), stat = "identity") +
  facet_wrap(~Service_Type)

Combined_CTC_by_date %>% # same as above, but line graph, coded
  filter(Date < "2019-11-26") %>%
  mutate(week = cut(Date, "week"))%>%
  mutate(week = as.Date(week)) %>%
  group_by(week, Service_Type)  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  ggplot()+
  geom_line(aes(y =Average_Daily_Activity, x = week,
                group = Service_Type, 
                color = Service_Type)) 

# should probably chart increase from same day/week/month last year...

Combined_CTC_by_date %>% # average daily Activity by month, service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  ggplot()+
  geom_line(aes(y =Average_Daily_Activity, x = month ,
                group = Service_Type, 
                color = Service_Type)) # consider adding month of year...

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  group_by(Service_Type) %>%
  mutate(Last_Year = dplyr::lag(Average_Daily_Activity, 12, order_by = month)) %>%
  mutate(Change = (Average_Daily_Activity - Last_Year)/ Last_Year) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(labels = scales::date_format("%b-%Y"))

library(scales)

Combined_CTC_by_date %>% # same as above, but flipped
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  group_by(Service_Type) %>%
  mutate(Last_Year = dplyr::lag(Average_Daily_Activity, 12, order_by = month)) %>%
  mutate(Change = (Average_Daily_Activity - Last_Year)/ Last_Year) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(date_breaks = "1 month",
                   labels = scales::date_format("%b-%Y")) +
  coord_flip()

# same as above, but only with change since 2016

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Activity, 36, order_by = month)) %>%
  mutate(Change = (Average_Daily_Activity - Three_Years_Ago)/ Three_Years_Ago) %>%
  mutate(month = as.POSIXct(month, "%Y-%m-%d")) %>%
  ggplot()+
  geom_line(aes(y =Change, x = month ,
                group = Service_Type, 
                color = Service_Type)) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_datetime(labels = scales::date_format("%b-%Y"))

# let's get average daily boardings in the five months following system opening,
# and compare to present time-span.

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Activity, 36, order_by = month)) %>%
  mutate(Change = (Average_Daily_Activity - Three_Years_Ago)/ Three_Years_Ago) %>%
  View()

# we should probably cut by quarter or something...

Combined_CTC_by_date %>% # Year-over-year growth, by service type
  filter(Date >= "2016-07-01") %>%
  mutate(month = cut(Date, "quarter"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Activity = mean(Activity)) %>%
  group_by(Service_Type) %>%
  mutate(Three_Years_Ago = dplyr::lag(Average_Daily_Activity, 9, order_by = month)) %>%
  mutate(Change = (Average_Daily_Activity - Three_Years_Ago)/ Three_Years_Ago) %>%
  View()

# let's strictly compare Septermber through November
# (this is not included in the boardings / alightings sections)

Combined_CTC_by_date %>% # Sept to Nov comp
  filter(Year == 2016 | Year == 2019 ,
           Month %in% c(9,10,11)) %>%
  group_by(Year, Service_Type) %>%
  summarise(Average_Daily_Activity = mean(Activity),
            Average_Daily_Boardings = mean(Boardings),
            Average_Daily_Alightings = mean(Alightings)) %>%
  formattable::formattable()

# results

# Year	Service_Type	Average_Daily_Activity	Average_Daily_Boardings	Average_Daily_Alightings
# 2016	Saturday	6447.167	4941.750	1505.417
# 2016	Sunday	3781.733	2820.733	961.000
# 2016	Weekday	11118.349	8602.841	2515.508
# 2019	Saturday	9798.000	5207.667	4590.333
# 2019	Sunday	6587.000	3533.786	3053.214
# 2019	Weekday	16415.210	8746.548	7668.661

# total change for average daily figures:

((16415.210 + 6587.000 + 9798.000) - (11118.349 + 3781.733 + 6447.167)) / (11118.349 + 3781.733 + 6447.167)

# weekday change, boardings

(8746.548 - 8602.841) /8602.841 

# saturday change, boardings

(5207.667 - 4941.750) / 4941.750 

# sunday change, boardings

(3533.786 - 2820.733) / 2820.733 

# Boardings (total average daily change)

((8746.548 +3533.786 +5207.667) - (8602.841 + 2820.733 + 4941.750)) / (8602.841 + 2820.733 + 4941.750)

Combined_CTC_by_date %>% # same as above, but average daily w/o service type
  filter(Year == 2016 | Year == 2019 ,
         Month %in% c(9,10,11)) %>%
  group_by(Year) %>%
  summarise(Average_Daily_Activity = mean(Activity),
            Average_Daily_Boardings = mean(Boardings),
            Average_Daily_Alightings = mean(Alightings)) %>%
  formattable::formattable()

# Year	Average_Daily_Activity	Average_Daily_Boardings	Average_Daily_Alightings
# 2016	9272.756	7151.011	2121.744
# 2019	13949.284	7434.670	6514.614

(13949.284 - 9272.756) / 9272.756 # activity, average daily

(7434.6704 - 7151.011) / 7151.011 # boarding, average daily

(6514.614 - 2121.744) / 2121.744 # alighting, average daily (huge!)



Combined_CTC_by_date %>% # same as above, but raw total
  filter(Year == 2016 | Year == 2019 ,
         Month %in% c(9,10,11)) %>%
  group_by(Year) %>%
  summarise(Total_Activity = sum(Activity),
            Total_Boardings = sum(Boardings),
            Total_Alightings = sum(Alightings)) %>%
  formattable::formattable()

# Year	Total_Activity	Total_Boardings	Total_Alightings
# 2016	834548	643591	190957
# 2019	1227537	654251	573286

(1227537 - 834548) / 834548 # total

(654251 - 643591) / 643591 # only 1 percent!

(573286 - 190957) / 190957 # huge!!

# need to verify alightings.. take a look at raw data..

# other_vehicles_dates_to_remove_Apc_raw <- Apc_raw %>% # why is this not working?
#   filter(MDCID == 1986, Transit_Day %in% c("2019-09-25", "2019-09-26",
#                                                 "2019-10-10", "2019-10-12"))


Combined_CTC_by_date %>% # note, only for CTC...
  group_by(Date) %>%
  summarise(Difference = sum(Boardings) - sum(Alightings)) %>%
  ggplot() +
  geom_line(aes(x = Date, y = Difference)) +
  labs(title = "Difference between daily boardings and alightings at the CTC",
       x = "")

Combined_CTC_by_date %>% # note, only for CTC...
  ggplot() +
  geom_line(aes(x = Date, y = Boardings.x, color = "Red")) +
  geom_line(aes(x = Date, y = Boardings.y, color = "Blue"))

Combined_CTC_by_date %>% # note, only for CTC...
  ggplot() +
  geom_line(aes(x = Date, y = Alightings.x, color = "Red")) +
  geom_line(aes(x = Date, y = Alightings.y, color = "Blue"))

pivot_longer(Combined_CTC_by_date, cols = c(Boardings.x, Boardings.y)) %>%
  View()

pivot_longer(Combined_CTC_by_date, cols = c(Boardings.x, Boardings.y)) %>%
  ggplot(aes(x = Date, y = value, fill = name))+
  geom_col(position = "stack")

pivot_longer(Combined_CTC_by_date, cols = c(Alightings.x, Alightings.y)) %>%
  ggplot(aes(x = Date, y = value, fill = name))+
  geom_col(position = "stack") +
  facet_wrap(~Service_Type)

Combined_CTC_by_date %>%
  mutate(month = cut(Date, "month"))%>%
  mutate(month  = as.Date(month)) %>%
  group_by(month , Service_Type)  %>%
  summarise(Average_Daily_Boardings = mean(Boardings)) %>%
  ggplot(aes(x = month, y = Average_Daily_Boardings, fill = Service_Type))+
  geom_col(position = "stack") # this looks nice but is not very accurate...

# (maybe look at system-wide alightings since start of 2019...)
# (Or... maybe it has to do with APC configuration???)
# (or just give Ed what we have and move on...)

############## 12/18/19 Update #################

# get boardings + alightings at all stops along 38th street

library(tidyverse)

### --- Database connection --- ###

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW",
                       Database = "DW_IndyGo", Port = 1433)

DimDate_raw <- tbl(con2, "DimDate") %>%
  filter(CalendarDate >= "2018-11-01", CalendarDate < "2019-12-01") %>%
  collect()

DimFare_APC <- tbl(con2, "DimFare") %>% # 
  filter(FareKey %in% c(1001, 1002, 1003)) %>%
  collect()

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

DimStop_38th <- filter(DimStop_raw, str_detect(StopDesc, "38th" ))

FactFare_38th <- tbl(con2, "FactFare") %>%
  filter(DateKey %in% !!DimDate_raw$DateKey,
         FareKey %in% c(1001, 1002, 1003),
         StopKey %in% !!DimStop_38th$StopKey) %>%
  collect()

DimVehicle_raw <- tbl(con2, "DimVehicle") %>% collect()

# now transform FactFare to be sensible...

FactFare_38th_joined <- FactFare_38th %>%
  left_join(select(DimFare_APC, FareKey, FareReportLabel), by = "FareKey") %>%
  left_join(select(DimStop_raw, StopKey, StopID, StopReportLabel, StopDesc), by = "StopKey") %>%
  left_join(select(DimVehicle_raw, VehicleKey, MDCID), by = "VehicleKey") %>%
  left_join(DimDate_raw, by = "DateKey")

FactFare_38th_sensible <- FactFare_38th_joined %>%
  pivot_wider(., id_cols = FareboxRecordID, names_from = FareReportLabel, 
              values_from = FareCount,
              names_sep = "")

FactFare_38th_sensible_revised <- FactFare_38th_joined %>%
  filter(FareReportLabel == "On Board") %>%
  left_join(FactFare_38th_sensible, by = "FareboxRecordID")

### transformations ###

# Get boardings/alightings/activity by stop.

FactFare_38th_sensible_revised %>%
  filter(!MDCID %in% c(1988, 1994, 1993, 1899)) %>%
  group_by(StopDesc) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Boardings_and_Alightings = sum(Boarding) + sum(Alighting)) %>%
  arrange(desc(Boardings_and_Alightings)) %>%
  View()

# graph it

FactFare_38th_sensible_revised %>%
  filter(!MDCID %in% c(1988, 1994, 1993, 1899)) %>%
  group_by(StopDesc) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Boardings_and_Alightings = sum(Boarding) + sum(Alighting)) %>%
  arrange(desc(Boardings_and_Alightings)) %>%
  ggplot()+
  geom_bar(aes(x = reorder(StopDesc, -Boardings), y = Boardings), stat = "identity")+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))

FactFare_38th_sensible_revised %>%
  filter(!MDCID %in% c(1988, 1994, 1993, 1899)) %>%
  group_by(StopDesc) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Boardings_and_Alightings = sum(Boarding) + sum(Alighting)) %>%
  arrange(desc(Boardings_and_Alightings)) %>%
  pivot_longer(cols = c(Boardings, Alightings)) %>%
  View()

FactFare_38th_sensible_revised %>% # for combined stops (e.g., both stops on street)
  filter(!MDCID %in% c(1988, 1994, 1993, 1899)) %>%
  group_by(StopDesc) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Boardings_and_Alightings = sum(Boarding) + sum(Alighting)) %>%
  arrange(desc(Boardings_and_Alightings)) %>%
  pivot_longer(cols = c(Boardings, Alightings)) %>%
  top_n(., 40, wt = Boardings_and_Alightings) %>%  
  ggplot(aes(fill = factor(name),
             x = reorder(StopDesc, desc(-Boardings_and_Alightings))))+
  geom_bar(aes(y = value),stat = "identity")+
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  coord_flip()+
  labs(title = "Busiest Stops along 38th Street", 
       subtitle = "Boardings + Alightings, November 2019",
       x = "Stop", y = "Boardings + Alightings") +
  theme(legend.title = element_blank())+
  theme(axis.text.x = element_text(angle = 0, hjust = 1))

Stop_BA <- FactFare_38th_sensible_revised %>%
  filter(!MDCID %in% c(1988, 1994, 1993, 1899)) %>%
  group_by(StopDesc) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Boardings_and_Alightings = sum(Boarding) + sum(Alighting)) %>%
  arrange(desc(Boardings_and_Alightings)) 

View(Stop_BA)

StopID_BA <- FactFare_38th_sensible_revised %>%
  filter(!MDCID %in% c(1988, 1994, 1993, 1899)) %>%
  group_by(StopDesc, StopID) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Boardings_and_Alightings = sum(Boarding) + sum(Alighting)) %>%
  arrange(desc(Boardings_and_Alightings)) 

View(StopID_BA)

# write.csv(Stop_BA, "Stop_Area_Boardings_and_Alightings.csv", row.names = FALSE)
# write.csv(StopID_BA, "Stop_ID_Boardings_and_Alightings.csv", row.names = FALSE)

library(leaflet)

stop_activity_stop <- FactFare_38th_sensible_revised %>%
  filter(!MDCID %in% c(1988, 1994, 1993, 1899)) %>%
  group_by(StopDesc, StopID) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting),
            Boardings_and_Alightings = sum(Boarding) + sum(Alighting)) %>%
  left_join(filter(DimStop_raw, StopActiveDesc == "Active", ActiveInd == 1), by = c("StopID"))

pal <- colorNumeric(
  palette = "Reds",
  domain = stop_activity$Boardings_and_Alightings)

operative_stops <- stop_activity$StopID

leaflet(stop_activity) %>%
  addProviderTiles(providers$Stamen.Toner) %>%
  addCircles(lat = ~Latitude,
             lng = ~Longitude,
             color = ~pal(Boardings_and_Alightings),
             radius = ~sqrt(Boardings_and_Alightings),
             highlightOptions = highlightOptions(color = "black",
                                                 weight = 6,
                                                 bringToFront = TRUE,
                                                 fillColor = "black",
                                                 fillOpacity = 1),
             fillOpacity = .5,
             label = paste("Stop ID ", 
                           stop_activity$StopID, 
                           ", Boardings + Alightings =", 
                           stop_activity$Boardings_and_Alightings),
             group = operative_stops ) %>%
  addLayersControl(
    # baseGroups = stop_activity$StopID,
    overlayGroups = operative_stops , # ehhh... this doesn't work..
    options = layersControlOptions(collapsed = FALSE))


leaflet(stop_activity) %>%
  addProviderTiles(providers$Stamen.Toner) %>%
  addCircles(lat = ~Latitude,
             lng = ~Longitude,
             radius = ~(Boardings_and_Alightings)/100,
             highlightOptions = highlightOptions(color = "black",
                                                 weight = 6,
                                                 bringToFront = TRUE,
                                                 fillColor = "black",
                                                 fillOpacity = .5),
             label = paste("Stop ID ", 
                             stop_activity$StopID, 
                             ", Boardings + Alightings =", 
                             stop_activity$Boardings_and_Alightings),
             fillOpacity = 1) %>%
  addLayersControl(
    baseGroups = stop_activity$StopID,
    overlayGroups = stop_activity$StopID,
    options = layersControlOptions(collapsed = FALSE)
  )

########## 12/19/19 Update #######

# Justin thinks Nov. 19th ridership data I provided was too low. 
# check other sources.

# First get 90 results. Then get Red Line results. Then get Apc_Data results, manaully cleared

##### Route 90 from November ######


library(tidyverse)
library(leaflet)
library(magrittr)
library(timeDate)

### --- Database Connections --- ###

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW",
                       Database = "DW_IndyGo",
                       Port = 1433)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

### --- Data Import --- ###

Vehicle_Message_History_raw_sample_90 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00",
         Route_Name == "90", Route == 90,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_sample_90 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00") %>%
  collect() # consider doing some semi_join or something 

# get stops

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_raw_sample_90 <- Vehicle_Message_History_raw_sample_90  %>%
  left_join(Vehicle_Avl_History_raw_sample_90, by = "Avl_History_Id")

# # Filter by latitude to create Red Line set
# 
# Vehicle_Message_History_raw_sample_90 <- Vehicle_Message_History_raw_sample_90 %>%
#   filter(Latitude > 39.709468, Latitude < 39.877512)

# Filter by longitude to remove garage boardings, if any

# Vehicle_Message_History_raw_sample_90 <- Vehicle_Message_History_raw_sample_90 %>%
#   filter(Longitude < -86.173321)

# format dates in VMH

Vehicle_Message_History_raw_sample_90$Date <- as.Date(str_sub(Vehicle_Message_History_raw_sample_90$Time.x, 1, 10))

Vehicle_Message_History_raw_sample_90$Clock_Time <- str_sub(Vehicle_Message_History_raw_sample_90$Time.x, 12, 19)

Vehicle_Message_History_raw_sample_90$DateTest <- ifelse(Vehicle_Message_History_raw_sample_90$Clock_Time < 
                                                           "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_raw_sample_90$Transit_Day_Unix <- ifelse(Vehicle_Message_History_raw_sample_90$DateTest == 1,
                                                                 lubridate::as_date(Vehicle_Message_History_raw_sample_90$Date - 1),
                                                                 Vehicle_Message_History_raw_sample_90$Date)

# add two dates together

Vehicle_Message_History_raw_sample_90$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_raw_sample_90$Transit_Day <- Vehicle_Message_History_raw_sample_90$Epoch_Date + lubridate::days(Vehicle_Message_History_raw_sample_90$Transit_Day_Unix)

# just get September data

Vehicle_Message_History_raw_sample_90 <- Vehicle_Message_History_raw_sample_90 %>%
  filter(Transit_Day >= "2019-11-01", Transit_Day < "2019-12-01")

# now set service types

# Vehicle_Message_History_raw_sample_90$Date <- as.Date(Vehicle_Message_History_raw_sample_90$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_raw_sample_90$Year <- lubridate::year(Vehicle_Message_History_raw_sample_90$Transit_Day)
Vehicle_Message_History_raw_sample_90$Month <- lubridate::month(Vehicle_Message_History_raw_sample_90$Transit_Day)
Vehicle_Message_History_raw_sample_90$Day <- lubridate::wday(Vehicle_Message_History_raw_sample_90$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_raw_sample_90_weekdays <- Vehicle_Message_History_raw_sample_90 %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_raw_sample_90_saturday <- Vehicle_Message_History_raw_sample_90 %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_raw_sample_90_sunday <- Vehicle_Message_History_raw_sample_90 %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_raw_sample_90_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_raw_sample_90_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_raw_sample_90_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_raw_sample_90 <- rbind(Vehicle_Message_History_raw_sample_90_weekdays, Vehicle_Message_History_raw_sample_90_sunday,
                                               Vehicle_Message_History_raw_sample_90_saturday)

# mutate calendar day for identifier:

Vehicle_Message_History_raw_sample_90$Transit_Day_for_mutate <- str_replace_all(Vehicle_Message_History_raw_sample_90$Transit_Day,"-", "")

# add unique trip identifier:

Vehicle_Message_History_raw_sample_90$AdHocUniqueTripNumber <- str_c(Vehicle_Message_History_raw_sample_90$Inbound_Outbound,
                                                                     Vehicle_Message_History_raw_sample_90$Transit_Day_for_mutate,
                                                                     Vehicle_Message_History_raw_sample_90$Trip,
                                                                     Vehicle_Message_History_raw_sample_90$Vehicle_ID,
                                                                     Vehicle_Message_History_raw_sample_90$Run_Id)

# now get clock hour

Vehicle_Message_History_raw_sample_90$Clock_Hour <- str_sub(Vehicle_Message_History_raw_sample_90$Clock_Time, 1, 2)

# get seconds since midnight

Vehicle_Message_History_raw_sample_90$seconds_between_dates <- difftime(Vehicle_Message_History_raw_sample_90$Date,
                                                                        Vehicle_Message_History_raw_sample_90$Transit_Day,
                                                                        units = "secs")

# maybe just need to use GPS_Time format...

Vehicle_Message_History_raw_sample_90$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_raw_sample_90$Time.x,
                                                                                  Vehicle_Message_History_raw_sample_90$Date, 
                                                                                  units = "secs")

# add those two seconds to get seconds since (true) midnight

Vehicle_Message_History_raw_sample_90$seconds_since_midnight <- Vehicle_Message_History_raw_sample_90$seconds_since_midnight_GPS_Time +
  Vehicle_Message_History_raw_sample_90$seconds_between_dates

# clean the format up (dplyr no like POSIXlt)

Vehicle_Message_History_raw_sample_90$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_raw_sample_90$seconds_since_midnight))

# now.... need to apply validation criteria to Vehicle_Message_History_raw_sample_90
# to develop set of valid/invalid trips...

# get days in which vehicles had no APC data

zero_boarding_alighting_vehicles <- Vehicle_Message_History_raw_sample_90 %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings == 0 | Alightings == 0)

# remove those vehicles on those days

Vehicle_Message_History_raw_sample_90_no_zero_boardings <- Vehicle_Message_History_raw_sample_90 %>%
  anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID"))# %>%
# filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

# review to verify this worked (look for 1993 or 1986)

zero_boarding_alighting_vehicles 

# looks good! 
# now we need to identify vehicles with wacky APCs.

# Note... Red for Ed event on Nov. 19th may have seen true, high ridership. 
# Will need to examine daily boardings.

Vehicle_Message_History_raw_sample_90_no_zero_boardings %>% 
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  arrange(Transit_Day) %>%
  View() # looks okay to me... (though 1993, 1994, and 1988 all look dicey)

# make variable that is two standard deviations from the mean

boardings_two_sd_from_mean <- (Vehicle_Message_History_raw_sample_90_no_zero_boardings %>% 
                                 group_by(Transit_Day, Vehicle_ID) %>%
                                 summarise(Boardings = sum(Boards),
                                           Alightings =sum(Alights)) %$%
                                 sd(Boardings)*2) + 
  (Vehicle_Message_History_raw_sample_90_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %$%
     mean(Boardings))

# get vehicles that are two sd from mean

wacky_apc_vehicles <- Vehicle_Message_History_raw_sample_90 %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_two_sd_from_mean)

# get summary of wacky vehicles

wacky_apc_vehicles %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boardings))

# build obvious outlier set (for Option 4 - step 1)

Vehicle_Message_History_raw_sample_90 %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 1250 # this still holds for November...

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample_90 %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

# and then get vehicle days above 2 SDs above mean (for Option 4 - step 2)

boardings_two_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_90_no_zero_boardings %>%
                                                     group_by(Transit_Day, Vehicle_ID) %>%
                                                     summarise(Boardings = sum(Boards),
                                                               Alightings = sum(Alights)) %>%
                                                     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                     sd(Boardings)*2) + 
  (Vehicle_Message_History_raw_sample_90_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample_90 %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_two_sd_from_mean_outlier_set_removed)

# option 5 - (permutation of option 4) - remove outliers and then vehicles whose boardings/alightings difference are >5%, for vehicles above 2 sds above mean

Vehicle_Message_History_raw_sample_90 %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 1250

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample_90 %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

boardings_two_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_90_no_zero_boardings %>%
                                                     group_by(Transit_Day, Vehicle_ID) %>%
                                                     summarise(Boardings = sum(Boards),
                                                               Alightings = sum(Alights)) %>%
                                                     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                     sd(Boardings)*2) + 
  (Vehicle_Message_History_raw_sample_90_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

high_end_wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample_90 %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  mutate(Percent_Difference = (Boardings - Alightings)/ ((Boardings + Alightings)/2))%>%
  filter(Boardings > obvious_outlier |
           (Boardings > boardings_two_sd_from_mean_outlier_set_removed) &
           Percent_Difference > 0.05 | 
           (Boardings > boardings_two_sd_from_mean_outlier_set_removed) &
           Percent_Difference < -0.05)

# remove those vehicles -- Option 1 -- remove vehicles ON THOSE DAYS
# 
# Vehicle_Message_History_raw_sample_90_clean <- Vehicle_Message_History_raw_sample_90 %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   anti_join(wacky_apc_vehicles, by = c("Transit_Day", "Vehicle_ID"))
# 
# # remove those vehicles -- Option 2 -- remove vehicles ON ALL DAYS

# Vehicle_Message_History_raw_sample_90_clean <- Vehicle_Message_History_raw_sample_90 %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   anti_join(wacky_apc_vehicles, by = c("Vehicle_ID"))

# remove those vehicles -- Option 3 -- remove vehicles ON THOSE DAYS, *plus* vehicles we know to be bad (1988)

# bad_vehicle_set <- c(1988)
# 
# Vehicle_Message_History_raw_sample_90_clean <- Vehicle_Message_History_raw_sample_90 %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   anti_join(wacky_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   filter(Vehicle_ID != bad_vehicle_set)

# remove those vehicles -- Option 4 -- remove obvious outliers, then apply option 1

# Vehicle_Message_History_raw_sample_90_clean <- Vehicle_Message_History_raw_sample_90 %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   anti_join(wacky_apc_vehicles_reiterated , by = c("Transit_Day", "Vehicle_ID"))

# remove those vehicles -- Option 5 -- remove obvious outliers, then 
# remove vehicles surpassing 5% threshold to vehicle-days w/ high boardings

Vehicle_Message_History_raw_sample_90_clean <- Vehicle_Message_History_raw_sample_90 %>%
  anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
  anti_join(high_end_wacky_apc_vehicles_reiterated , by = c("Transit_Day", "Vehicle_ID"))

# # now build dataset of invalid data -- Option 3 (see above) AND Option 4 (see above) AND Option 5
# 
Vehicle_Message_History_raw_sample_90_invalid <- Vehicle_Message_History_raw_sample_90 %>%
  anti_join(Vehicle_Message_History_raw_sample_90_clean)

# now let's do the expansion method...

valid_df <- Vehicle_Message_History_raw_sample_90_clean %>% # valid data
  group_by(AdHocUniqueTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
  arrange(AdHocUniqueTripNumber, seconds_since_midnight) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
  summarise(Valid_Boardings = sum(Boards),
            Valid_Trips = n_distinct(AdHocUniqueTripNumber)) 

valid_pass_throughs <- Vehicle_Message_History_raw_sample_90_clean %>% # valid data for pass-throughs
  group_by(AdHocUniqueTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
  mutate(First_Onboard_Count = first(Onboard),
         First_Boarding = first(Boards),
         First_Alighting = first(Alights),
         Starting_Pass_Through = first(Onboard) - first(Boards) + first(Alights)) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type, AdHocUniqueTripNumber) %>%
  summarise(Starting_Pass_Through_grouped = first(Starting_Pass_Through)) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
  summarise(Starting_Pass_Through_to_add = sum(Starting_Pass_Through_grouped))

invalid_df <- Vehicle_Message_History_raw_sample_90_invalid  %>% # invalid data
  group_by(AdHocUniqueTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
  arrange(AdHocUniqueTripNumber, seconds_since_midnight) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
  summarise(Invalid_Boardings = sum(Boards),
            Invalid_Trips = n_distinct(AdHocUniqueTripNumber))

# left_join(valid_df, invalid_df,
#           by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
#   mutate(Boardings_to_add = ifelse(!is.na(Invalid_Trips),
#                                    Invalid_Trips * Boardings_per_valid_trip,
#                                    0)) %>%
#   mutate(Expanded_boardings = Boardings_to_add + Valid_Boardings) %>%
#   View() # may want to think a little more about the join here....

results_90 <- left_join(valid_df, invalid_df,
                        by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
  left_join(., valid_pass_throughs , by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
  mutate(Boardings_per_valid_trip = (Valid_Boardings +Starting_Pass_Through_to_add) / Valid_Trips) %>%
  mutate(Boardings_to_add = ifelse(!is.na(Invalid_Trips),
                                   Invalid_Trips * Boardings_per_valid_trip,
                                   Starting_Pass_Through_to_add)) %>%
  mutate(Expanded_boardings = Boardings_to_add + Valid_Boardings)  %>%
  mutate(Percent_Invalid_Trips  = Invalid_Trips / (Invalid_Trips + Valid_Trips))

# to get RL ridership: 

sum(results_90$Expanded_boardings, na.rm = TRUE)

sum(results_90$Valid_Trips, na.rm = TRUE)
sum(results_90$Invalid_Trips, na.rm = TRUE)

sum(results_90$Valid_Trips, na.rm = TRUE)/ (sum(results_90$Valid_Trips, na.rm = TRUE) + sum(results_90$Invalid_Trips, na.rm = TRUE))

results_90 %>%
  arrange(Inbound_Outbound, Service_Type, Trip_Start_Hour) %>%
  View()

# result: 182990.9 boardings with 0.9564481 valid data...

# what do results_90 look like if we don't include pass-throughs? 
# (this is what we used for October but not September!)

alternative_results_90 <- left_join(valid_df, invalid_df,
                                    by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
  mutate(Boardings_per_valid_trip = (Valid_Boardings) / Valid_Trips) %>%
  mutate(Boardings_to_add = ifelse(!is.na(Invalid_Trips),
                                   Invalid_Trips * Boardings_per_valid_trip,
                                   0)) %>%
  mutate(Expanded_boardings = Boardings_to_add + Valid_Boardings)  %>%
  mutate(Percent_Invalid_Trips  = Invalid_Trips / (Invalid_Trips + Valid_Trips))

# to get route 90 ridership: 

sum(alternative_results_90$Expanded_boardings, na.rm = TRUE)

sum(alternative_results_90$Valid_Trips, na.rm = TRUE)
sum(alternative_results_90$Invalid_Trips, na.rm = TRUE)

sum(alternative_results_90$Valid_Trips, na.rm = TRUE)/ (sum(alternative_results_90$Valid_Trips, na.rm = TRUE) + sum(alternative_results_90$Invalid_Trips, na.rm = TRUE))

# alternative results_90 produces ridership of 179757.2 for 0.9564481 valid trips.

# what is raw sum?

sum(Vehicle_Message_History_raw_sample_90_clean$Boards)

##### Get boardings per day for Route 90 ####

Vehicle_Message_History_raw_sample_90_clean %>%
  group_by(Transit_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  View() # 9094 for November 19th.

# review bad data to ensure nothing improper was cut out.

high_end_wacky_apc_vehicles_reiterated %>%
  filter(Transit_Day == "2019-11-19") %>%
  View() # hmm....

high_end_wacky_apc_vehicles_reiterated %>%
  View()

##### Red Line from November ######     

# Libraries

library(tidyverse)
library(leaflet)
library(magrittr)
library(timeDate)

### --- Database Connections --- ###

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW",
                       Database = "DW_IndyGo",
                       Port = 1433)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

### --- Data Import --- ###

Vehicle_Message_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00",
         Route_Name == "90", Route == 90,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-10-30 03:00:00", Time < "2019-12-02 03:00:00") %>%
  collect() # consider doing some semi_join or something 

# get stops

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample  %>%
  left_join(Vehicle_Avl_History_raw_sample, by = "Avl_History_Id")

# Filter by latitude to create Red Line set

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Latitude > 39.709468, Latitude < 39.877512)

# Filter by longitude to remove garage boardings, if any

# Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
#   filter(Longitude < -86.173321)

# format dates in VMH

Vehicle_Message_History_raw_sample$Date <- as.Date(str_sub(Vehicle_Message_History_raw_sample$Time.x, 1, 10))

Vehicle_Message_History_raw_sample$Clock_Time <- str_sub(Vehicle_Message_History_raw_sample$Time.x, 12, 19)

Vehicle_Message_History_raw_sample$DateTest <- ifelse(Vehicle_Message_History_raw_sample$Clock_Time < 
                                                        "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_raw_sample$Transit_Day_Unix <- ifelse(Vehicle_Message_History_raw_sample$DateTest == 1,
                                                              lubridate::as_date(Vehicle_Message_History_raw_sample$Date - 1),
                                                              Vehicle_Message_History_raw_sample$Date)

# add two dates together

Vehicle_Message_History_raw_sample$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_raw_sample$Transit_Day <- Vehicle_Message_History_raw_sample$Epoch_Date + lubridate::days(Vehicle_Message_History_raw_sample$Transit_Day_Unix)

# just get September data

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Transit_Day >= "2019-11-01", Transit_Day < "2019-12-01")

# now set service types

# Vehicle_Message_History_raw_sample$Date <- as.Date(Vehicle_Message_History_raw_sample$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_raw_sample$Year <- lubridate::year(Vehicle_Message_History_raw_sample$Transit_Day)
Vehicle_Message_History_raw_sample$Month <- lubridate::month(Vehicle_Message_History_raw_sample$Transit_Day)
Vehicle_Message_History_raw_sample$Day <- lubridate::wday(Vehicle_Message_History_raw_sample$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_raw_sample_weekdays <- Vehicle_Message_History_raw_sample %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_raw_sample_saturday <- Vehicle_Message_History_raw_sample %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_raw_sample_sunday <- Vehicle_Message_History_raw_sample %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_raw_sample_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_raw_sample_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_raw_sample_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_raw_sample <- rbind(Vehicle_Message_History_raw_sample_weekdays, Vehicle_Message_History_raw_sample_sunday,
                                            Vehicle_Message_History_raw_sample_saturday)

# mutate calendar day for identifier:

Vehicle_Message_History_raw_sample$Transit_Day_for_mutate <- str_replace_all(Vehicle_Message_History_raw_sample$Transit_Day,"-", "")

# add unique trip identifier:

Vehicle_Message_History_raw_sample$AdHocUniqueTripNumber <- str_c(Vehicle_Message_History_raw_sample$Inbound_Outbound,
                                                                  Vehicle_Message_History_raw_sample$Transit_Day_for_mutate,
                                                                  Vehicle_Message_History_raw_sample$Trip,
                                                                  Vehicle_Message_History_raw_sample$Vehicle_ID,
                                                                  Vehicle_Message_History_raw_sample$Run_Id)

# now get clock hour

Vehicle_Message_History_raw_sample$Clock_Hour <- str_sub(Vehicle_Message_History_raw_sample$Clock_Time, 1, 2)

# get seconds since midnight

Vehicle_Message_History_raw_sample$seconds_between_dates <- difftime(Vehicle_Message_History_raw_sample$Date,
                                                                     Vehicle_Message_History_raw_sample$Transit_Day,
                                                                     units = "secs")

# maybe just need to use GPS_Time format...

Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_raw_sample$Time.x,
                                                                               Vehicle_Message_History_raw_sample$Date, 
                                                                               units = "secs")

# add those two seconds to get seconds since (true) midnight

Vehicle_Message_History_raw_sample$seconds_since_midnight <- Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time +
  Vehicle_Message_History_raw_sample$seconds_between_dates

# clean the format up (dplyr no like POSIXlt)

Vehicle_Message_History_raw_sample$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_raw_sample$seconds_since_midnight))

# now.... need to apply validation criteria to Vehicle_Message_History_raw_sample
# to develop set of valid/invalid trips...

# get days in which vehicles had no APC data

zero_boarding_alighting_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings == 0 | Alightings == 0)

# remove those vehicles on those days

Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
  anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID"))# %>%
# filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

# review to verify this worked (look for 1993 or 1986)

zero_boarding_alighting_vehicles 

# looks good! 
# now we need to identify vehicles with wacky APCs.

# Note... Red for Ed event on Nov. 19th may have seen true, high ridership. 
# Will need to examine daily boardings.

Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  arrange(Transit_Day) %>%
  View() # looks okay to me... (though 1993, 1994, and 1988 all look dicey)

# make variable that is two standard deviations from the mean

boardings_two_sd_from_mean <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
                                 group_by(Transit_Day, Vehicle_ID) %>%
                                 summarise(Boardings = sum(Boards),
                                           Alightings =sum(Alights)) %$%
                                 sd(Boardings)*2) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %$%
     mean(Boardings))

# get vehicles that are two sd from mean

wacky_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_two_sd_from_mean)

# get summary of wacky vehicles

wacky_apc_vehicles %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boardings))

# build obvious outlier set (for Option 4 - step 1)

Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 1250 # this still holds for November...

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

# and then get vehicle days above 2 SDs above mean (for Option 4 - step 2)

boardings_two_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
                                                     group_by(Transit_Day, Vehicle_ID) %>%
                                                     summarise(Boardings = sum(Boards),
                                                               Alightings = sum(Alights)) %>%
                                                     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                     sd(Boardings)*2) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_two_sd_from_mean_outlier_set_removed)

# option 5 - (permutation of option 4) - remove outliers and then vehicles whose boardings/alightings difference are >5%, for vehicles above 2 sds above mean

Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 1250

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

boardings_two_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
                                                     group_by(Transit_Day, Vehicle_ID) %>%
                                                     summarise(Boardings = sum(Boards),
                                                               Alightings = sum(Alights)) %>%
                                                     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                     sd(Boardings)*2) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

high_end_wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  mutate(Percent_Difference = (Boardings - Alightings)/ ((Boardings + Alightings)/2))%>%
  filter(Boardings > obvious_outlier |
           (Boardings > boardings_two_sd_from_mean_outlier_set_removed) &
           Percent_Difference > 0.05 | 
           (Boardings > boardings_two_sd_from_mean_outlier_set_removed) &
           Percent_Difference < -0.05)

# remove those vehicles -- Option 1 -- remove vehicles ON THOSE DAYS
# 
# Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   anti_join(wacky_apc_vehicles, by = c("Transit_Day", "Vehicle_ID"))
# 
# # remove those vehicles -- Option 2 -- remove vehicles ON ALL DAYS

# Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   anti_join(wacky_apc_vehicles, by = c("Vehicle_ID"))

# remove those vehicles -- Option 3 -- remove vehicles ON THOSE DAYS, *plus* vehicles we know to be bad (1988)

# bad_vehicle_set <- c(1988)
# 
# Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   anti_join(wacky_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   filter(Vehicle_ID != bad_vehicle_set)

# remove those vehicles -- Option 4 -- remove obvious outliers, then apply option 1

# Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
#   anti_join(wacky_apc_vehicles_reiterated , by = c("Transit_Day", "Vehicle_ID"))

# remove those vehicles -- Option 5 -- remove obvious outliers, then 
# remove vehicles surpassing 5% threshold to vehicle-days w/ high boardings

Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample %>%
  anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")) %>%
  anti_join(high_end_wacky_apc_vehicles_reiterated , by = c("Transit_Day", "Vehicle_ID"))

# # now build dataset of invalid data -- Option 1 (see above)
# 
# Vehicle_Message_History_raw_sample_invalid <- rbind(
# 
#   Vehicle_Message_History_raw_sample %>%
#     semi_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")),
# 
#   Vehicle_Message_History_raw_sample %>%
#     semi_join(wacky_apc_vehicles, by = c("Transit_Day", "Vehicle_ID"))
# )
# 
# now build dataset of invalid data -- Option 2 (see above)

# Vehicle_Message_History_raw_sample_invalid <- rbind(
# 
#   Vehicle_Message_History_raw_sample %>%
#     semi_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID")),
# 
#   Vehicle_Message_History_raw_sample %>%
#     semi_join(wacky_apc_vehicles, by = c("Vehicle_ID"))
# )

# # now build dataset of invalid data -- Option 3 (see above) AND Option 4 (see above) AND Option 5
# 
Vehicle_Message_History_raw_sample_invalid <- Vehicle_Message_History_raw_sample %>%
  anti_join(Vehicle_Message_History_raw_sample_clean)

# now let's do the expansion method...

valid_df <- Vehicle_Message_History_raw_sample_clean %>% # valid data
  group_by(AdHocUniqueTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
  arrange(AdHocUniqueTripNumber, seconds_since_midnight) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
  summarise(Valid_Boardings = sum(Boards),
            Valid_Trips = n_distinct(AdHocUniqueTripNumber)) 

valid_pass_throughs <- Vehicle_Message_History_raw_sample_clean %>% # valid data for pass-throughs
  group_by(AdHocUniqueTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
  mutate(First_Onboard_Count = first(Onboard),
         First_Boarding = first(Boards),
         First_Alighting = first(Alights),
         Starting_Pass_Through = first(Onboard) - first(Boards) + first(Alights)) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type, AdHocUniqueTripNumber) %>%
  summarise(Starting_Pass_Through_grouped = first(Starting_Pass_Through)) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
  summarise(Starting_Pass_Through_to_add = sum(Starting_Pass_Through_grouped))

invalid_df <- Vehicle_Message_History_raw_sample_invalid  %>% # invalid data
  group_by(AdHocUniqueTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
  arrange(AdHocUniqueTripNumber, seconds_since_midnight) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
  summarise(Invalid_Boardings = sum(Boards),
            Invalid_Trips = n_distinct(AdHocUniqueTripNumber))

# left_join(valid_df, invalid_df,
#           by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
#   mutate(Boardings_to_add = ifelse(!is.na(Invalid_Trips),
#                                    Invalid_Trips * Boardings_per_valid_trip,
#                                    0)) %>%
#   mutate(Expanded_boardings = Boardings_to_add + Valid_Boardings) %>%
#   View() # may want to think a little more about the join here....

results <- left_join(valid_df, invalid_df,
                     by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
  left_join(., valid_pass_throughs , by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
  mutate(Boardings_per_valid_trip = (Valid_Boardings +Starting_Pass_Through_to_add) / Valid_Trips) %>%
  mutate(Boardings_to_add = ifelse(!is.na(Invalid_Trips),
                                   Invalid_Trips * Boardings_per_valid_trip,
                                   Starting_Pass_Through_to_add)) %>%
  mutate(Expanded_boardings = Boardings_to_add + Valid_Boardings)  %>%
  mutate(Percent_Invalid_Trips  = Invalid_Trips / (Invalid_Trips + Valid_Trips))

# to get RL ridership: 

sum(results$Expanded_boardings, na.rm = TRUE) # 164639 for November

sum(results$Valid_Trips, na.rm = TRUE)
sum(results$Invalid_Trips, na.rm = TRUE)

sum(results$Valid_Trips, na.rm = TRUE)/ (sum(results$Valid_Trips, na.rm = TRUE) + sum(results$Invalid_Trips, na.rm = TRUE))

results %>%
  arrange(Inbound_Outbound, Service_Type, Trip_Start_Hour) %>%
  View()

# result: 164679.8 boardings with 0.958213 valid data...

# what do results look like if we don't include pass-throughs? 
# (this is what we used for October but not September!)

alternative_results <- left_join(valid_df, invalid_df,
                                 by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
  mutate(Boardings_per_valid_trip = (Valid_Boardings) / Valid_Trips) %>%
  mutate(Boardings_to_add = ifelse(!is.na(Invalid_Trips),
                                   Invalid_Trips * Boardings_per_valid_trip,
                                   0)) %>%
  mutate(Expanded_boardings = Boardings_to_add + Valid_Boardings)  %>%
  mutate(Percent_Invalid_Trips  = Invalid_Trips / (Invalid_Trips + Valid_Trips))

# to get RL ridership: 

sum(alternative_results$Expanded_boardings, na.rm = TRUE)

sum(alternative_results$Valid_Trips, na.rm = TRUE)
sum(alternative_results$Invalid_Trips, na.rm = TRUE)

sum(alternative_results$Valid_Trips, na.rm = TRUE)/ (sum(alternative_results$Valid_Trips, na.rm = TRUE) + sum(alternative_results$Invalid_Trips, na.rm = TRUE))

# alternative results produces ridership of 161337.2 for 0.958213 valid trips. (November)

# what is raw sum?

sum(Vehicle_Message_History_raw_sample_clean$Boards)

# what is raw sum from unattributed stops?

Vehicle_Message_History_raw_sample_clean %>%
  filter(Stop_Id == 0) %$%
  sum(Boards) # 6503

##### Get boardings per day for Red Line ###

Vehicle_Message_History_raw_sample_clean %>%
  group_by(Transit_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  View() # 9094 for November 19th for Route 90; 7767 for Red Line

# review bad data to ensure nothing improper was cut out.

high_end_wacky_apc_vehicles_reiterated %>%
  filter(Transit_Day == "2019-11-19") %>%
  View() # hmm....

high_end_wacky_apc_vehicles_reiterated %>%
  View()

########### 1/2/20 Update ###########

library(tidyverse)
library(lubridate)
library(magrittr)

# try to get up Apc_Data

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_NYE <- tbl(con, "Apc_Data") %>%
  filter(GPS_Time >= "2019-12-29", GPS_Time <= "2020-01-02") %>%
  collect()

Apc_NYE$Date <- as.Date(str_sub(Apc_NYE$GPS_Time, 1, 10))

Apc_NYE$Clock_Time <- str_sub(Apc_NYE$GPS_Time, 12, 19)

Apc_NYE$DateTest <- ifelse(Apc_NYE$Clock_Time < 
                             "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_NYE$Transit_Day_Unix <- ifelse(Apc_NYE$DateTest == 1,
                                   lubridate::as_date(Apc_NYE$Date - 1),
                                   Apc_NYE$Date)

# add two dates together

Apc_NYE$Epoch_Date <- as.Date("1970-01-01")

Apc_NYE$Transit_Day <- Apc_NYE$Epoch_Date + lubridate::days(Apc_NYE$Transit_Day_Unix)

# filter for 8pm to 2am

Apc_NYE_8pm <- Apc_NYE %>%
  filter(Clock_Time >= "20:00:00" | Clock_Time <= "02:00:00")

# filter for day before NYE

Apc_day_before_NYE <- Apc_NYE_8pm %>%
  filter(Transit_Day == "2019-12-30")

# filter for NYE only

Apc_NYE_8pm <- Apc_NYE_8pm %>%
  filter(Transit_Day == "2019-12-31")

# get raw boardings

sum(Apc_NYE_8pm$Boarding)
sum(Apc_day_before_NYE$Boarding)

(sum(Apc_NYEApc_NYE_8pm$Boarding) - sum(Apc_day_before_NYE$Boarding)) / sum(Apc_day_before_NYE$Boarding)

# review for bad busses...

Apc_NYE_8pm %>%
  group_by(MDCID) %>%
  summarise(Boardings = sum(Boarding)) %>%
  arrange(desc(Boardings))

Apc_day_before_NYE %>%
  group_by(MDCID) %>%
  summarise(Boardings = sum(Boarding)) %>%
  arrange(desc(Boardings))

# what if we removed deadhead?

Apc_NYE_8pm %>%
  filter(Route != 999) %$%
  sum(Boarding)

Apc_day_before_NYE %>%
  filter(Route != 999) %$%
  sum(Boarding)

# very similar!

# what about over course of the day?

Apc_NYE$Hour_of_Day <- str_sub(Apc_NYE$Clock_Time, 1, 2)

# get seconds since midnight

Apc_NYE$seconds_between_dates <- difftime(Apc_NYE$Date,
                                          Apc_NYE$Transit_Day,
                                          units = "secs")

# maybe just need to use GPS_Time format...

Apc_NYE$seconds_since_midnight_GPS_Time <- difftime(Apc_NYE$GPS_Time, 
                                                    Apc_NYE$Date,
                                                    units = "secs")

# add those two seconds to get seconds since (true) midnight

Apc_NYE$seconds_since_midnight <- Apc_NYE$seconds_since_midnight_GPS_Time +
  Apc_NYE$seconds_between_dates

# clean the format up (dplyr no like POSIXlt)

Apc_NYE$seconds_since_midnight_in_seconds <- as.numeric(as.character(Apc_NYE$seconds_since_midnight))

# graph a little bit

Apc_NYE %>%
  filter(Clock_Time >= "20:00:00" | Clock_Time <= "02:00:00") %>%
  filter(Transit_Day == "2019-12-30" | Transit_Day == "2019-12-31") %>%
  group_by(Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  ggplot()+
  geom_bar(stat = "identity", aes(x = Hour_of_Day, y = Boardings))+
  geom_label(aes(x = Hour_of_Day, 
                y = Boardings, 
            label = Boardings), stat = "identity")+
  facet_wrap(~Transit_Day) +
  coord_flip()

#### 1/16/20 ####
#Data for Justin S.

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_Justin_Stops <- tbl(con, "Apc_Data") %>%
  filter(GPS_Time >= "2019-11-30", GPS_Time <= "2020-01-01",
         Stop == 10304 | Stop == 10353) %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_Justin_Stops$Date <- as.Date(str_sub(Apc_Justin_Stops$GPS_Time, 1, 10))

Apc_Justin_Stops$Clock_Time <- str_sub(Apc_Justin_Stops$GPS_Time, 12, 19)

Apc_Justin_Stops$DateTest <- ifelse(Apc_Justin_Stops$Clock_Time < 
                             "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_Justin_Stops$Transit_Day_Unix <- ifelse(Apc_Justin_Stops$DateTest == 1,
                                   lubridate::as_date(Apc_Justin_Stops$Date - 1),
                                   Apc_Justin_Stops$Date)

# add two dates together

Apc_Justin_Stops$Epoch_Date <- as.Date("1970-01-01")

Apc_Justin_Stops$Transit_Day <- Apc_Justin_Stops$Epoch_Date + lubridate::days(Apc_Justin_Stops$Transit_Day_Unix)

# get day of week

Apc_Justin_Stops$Day <- lubridate::wday(Apc_Justin_Stops$Date, label = TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2019, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2019, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Apc_Justin_Stops_weekdays <- Apc_Justin_Stops %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Apc_Justin_Stops_saturday <- Apc_Justin_Stops %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Apc_Justin_Stops_sunday <- Apc_Justin_Stops %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Apc_Justin_Stops_sunday$Service_Type <- "Sunday"
Apc_Justin_Stops_saturday$Service_Type <- "Saturday"
Apc_Justin_Stops_weekdays$Service_Type <- "Weekday"

# rebind

Apc_Justin_Stops <- rbind(Apc_Justin_Stops_weekdays, Apc_Justin_Stops_sunday,
                                               Apc_Justin_Stops_saturday)


# clean for December

Apc_Justin_Stops <- Apc_Justin_Stops %>%
  filter(Transit_Day >= "2019-12-01", Transit_Day < "2020-01-01")

# get service per day by stop

Apc_Justin_Stops %>%
  group_by(Stop, Service_Type) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting)) %>%
  arrange(Stop) %>%
  View()

Apc_Justin_Stops %>%
  group_by(Transit_Day, Stop, Service_Type) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting)) %>%
  group_by(Stop, Service_Type) %>%
  summarise(Average_Daily_Boardings = mean(Boardings),
            Average_Daily_Alightings = mean(Alightings)) %>%
  arrange(Stop) %>%
  View()

# review vehicles

sort(unique(Apc_Justin_Stops$MDCID))

#### 1/22/20 Ryan G request ####

library(tidyverse)
library(lubridate)
library(timeDate)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_RyanG_Stops <- tbl(con, "Apc_Data") %>%
  filter(GPS_Time >= "2019-08-30", GPS_Time <= "2020-01-02",
         Stop %in% c(12460,
                     51649,
                     51704,
                     51659,
                     51658,
                     51657)) %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_RyanG_Stops$Date <- as.Date(str_sub(Apc_RyanG_Stops$GPS_Time, 1, 10))

Apc_RyanG_Stops$Clock_Time <- str_sub(Apc_RyanG_Stops$GPS_Time, 12, 19)

Apc_RyanG_Stops$DateTest <- ifelse(Apc_RyanG_Stops$Clock_Time < 
                                      "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_RyanG_Stops$Transit_Day_Unix <- ifelse(Apc_RyanG_Stops$DateTest == 1,
                                            lubridate::as_date(Apc_RyanG_Stops$Date - 1),
                                            Apc_RyanG_Stops$Date)

# add two dates together

Apc_RyanG_Stops$Epoch_Date <- as.Date("1970-01-01")

Apc_RyanG_Stops$Transit_Day <- Apc_RyanG_Stops$Epoch_Date + lubridate::days(Apc_RyanG_Stops$Transit_Day_Unix)

# get day of week

Apc_RyanG_Stops$Day <- lubridate::wday(Apc_RyanG_Stops$Date, label = TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2019, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2019, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Apc_RyanG_Stops_weekdays <- Apc_RyanG_Stops %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Apc_RyanG_Stops_saturday <- Apc_RyanG_Stops %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Apc_RyanG_Stops_sunday <- Apc_RyanG_Stops %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Apc_RyanG_Stops_sunday$Service_Type <- "Sunday"
Apc_RyanG_Stops_saturday$Service_Type <- "Saturday"
Apc_RyanG_Stops_weekdays$Service_Type <- "Weekday"

# rebind

Apc_RyanG_Stops <- rbind(Apc_RyanG_Stops_weekdays, Apc_RyanG_Stops_sunday,
                          Apc_RyanG_Stops_saturday)

# clean for September through December

Apc_RyanG_Stops <- Apc_RyanG_Stops %>%
  filter(Transit_Day >= "2019-09-01", Transit_Day < "2020-01-01")

# get service per day by stop

Apc_RyanG_Stops %>%
  group_by(Stop, Service_Type) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting)) %>%
  arrange(Stop) %>%
  View()

Apc_RyanG_Stops %>%
  group_by(Transit_Day, Stop, Service_Type) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting)) %>%
  group_by(Stop, Service_Type) %>%
  summarise(Average_Daily_Boardings = mean(Boardings),
            Average_Daily_Alightings = mean(Alightings)) %>%
  arrange(Stop) %>%
  View()

Apc_RyanG_Stops %>% # this is it
  group_by(Transit_Day, Stop) %>%
  summarise(Boardings = sum(Boarding),
            Alightings = sum(Alighting)) %>%
  group_by(Stop) %>%
  summarise(Average_Daily_Boardings = round(mean(Boardings), digits = 1),
            Average_Daily_Alightings = round(mean(Alightings), digits = 1)) %>%
  arrange(Stop) %>%
  formattable::formattable()

# review vehicles

sort(unique(Apc_RyanG_Stops$MDCID))

#### 3/18/20 Update ####

# (moved to it's own script - Recent COVID ridership)

#### 3/25/20 Update ####

# Total ridership by trip per route from March 23rd / 24th
# - Use APC_Data first, and then use FF, if available..
# Needs route, time and direction (if available)


con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_Data_recent <- tbl(con, "Apc_Data") %>%
  filter(GPS_Time >= "2020-03-22 03:00:00") %>%
  collect()

# get transit day (for Sunday service, does not run past midnight, so this may not
# change much here)

Apc_Data_recent$Date <- as.Date(str_sub(Apc_Data_recent$GPS_Time, 1, 10))

Apc_Data_recent$Clock_Time <- str_sub(Apc_Data_recent$GPS_Time, 12, 19)

Apc_Data_recent$DateTest <- ifelse(Apc_Data_recent$Clock_Time < 
                                      "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_Data_recent$Transit_Day_Unix <- ifelse(Apc_Data_recent$DateTest == 1,
                                            lubridate::as_date(Apc_Data_recent$Date - 1),
                                            Apc_Data_recent$Date)

# add two dates together

Apc_Data_recent$Epoch_Date <- as.Date("1970-01-01")

Apc_Data_recent$Transit_Day <- Apc_Data_recent$Epoch_Date + lubridate::days(Apc_Data_recent$Transit_Day_Unix)

# do date check

Apc_Data_recent %>%
  group_by(Transit_Day) %>%
  summarise(n = n())

# get rid of 22nd.

Apc_Data_recent <- Apc_Data_recent %>%
  filter(Transit_Day >= "2020-03-23")

# do a vehicle check

Apc_Data_recent %>%
  group_by(MDCID) %>%
  summarise(B = sum(Boarding),
            A = sum(Alighting)) %>%
  arrange(desc(B))

# now get boardings per trip

Apc_Data_recent %>%
  group_by(Route, Trip) %>%
  summarise(Total_Boardings = sum(Boarding)) %>%
  arrange(desc(Total_Boardings)) %>%
  View()

# looks good. export

Apc_Data_recent %>%
  group_by(Route, Trip) %>%
  summarise(Total_Boardings = sum(Boarding)) %>%
  arrange(desc(Total_Boardings)) %>%
  write.csv(., file = "Apc_Data_Boardings_032320_032420.csv",
            row.names = FALSE)

#### 4/21/20 ####

library(tidyverse)
library(timeDate)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

Vehicle_Message_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-12-31 03:00:00", Time < "2020-03-02 03:00:00",
         Route_Name == "90", Route == 90,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-12-31 03:00:00", Time < "2020-03-02 03:00:00") %>%
  collect() # consider doing some semi_join or something 

### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample  %>%
  left_join(Vehicle_Avl_History_raw_sample, by = "Avl_History_Id")

# Filter by latitude to create Red Line set

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Latitude > 39.709468, Latitude < 39.877512)

# Filter by longitude to remove garage boardings, if any

# Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
#   filter(Longitude > -86.173321) # not sure if this is right...

# format dates in VMH

Vehicle_Message_History_raw_sample$Date <- as.Date(str_sub(Vehicle_Message_History_raw_sample$Time.x, 1, 10))

Vehicle_Message_History_raw_sample$Clock_Time <- str_sub(Vehicle_Message_History_raw_sample$Time.x, 12, 19)

Vehicle_Message_History_raw_sample$DateTest <- ifelse(Vehicle_Message_History_raw_sample$Clock_Time < 
                                                        "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_raw_sample$Transit_Day_Unix <- ifelse(Vehicle_Message_History_raw_sample$DateTest == 1,
                                                              lubridate::as_date(Vehicle_Message_History_raw_sample$Date - 1),
                                                              Vehicle_Message_History_raw_sample$Date)

# add two dates together

Vehicle_Message_History_raw_sample$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_raw_sample$Transit_Day <- Vehicle_Message_History_raw_sample$Epoch_Date + lubridate::days(Vehicle_Message_History_raw_sample$Transit_Day_Unix)

# just get February data

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Transit_Day >= "2020-01-01", Transit_Day < "2020-03-01")

# confirm dates

sort(unique(Vehicle_Message_History_raw_sample$Transit_Day))

# now set service types

# Vehicle_Message_History_raw_sample$Date <- as.Date(Vehicle_Message_History_raw_sample$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_raw_sample$Year <- lubridate::year(Vehicle_Message_History_raw_sample$Transit_Day)
Vehicle_Message_History_raw_sample$Month <- lubridate::month(Vehicle_Message_History_raw_sample$Transit_Day)
Vehicle_Message_History_raw_sample$Day <- lubridate::wday(Vehicle_Message_History_raw_sample$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_raw_sample_weekdays <- Vehicle_Message_History_raw_sample %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_raw_sample_saturday <- Vehicle_Message_History_raw_sample %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_raw_sample_sunday <- Vehicle_Message_History_raw_sample %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_raw_sample_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_raw_sample_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_raw_sample_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_raw_sample <- rbind(Vehicle_Message_History_raw_sample_weekdays, Vehicle_Message_History_raw_sample_sunday,
                                            Vehicle_Message_History_raw_sample_saturday)

# # mutate calendar day for identifier:
# 
# Vehicle_Message_History_raw_sample$Transit_Day_for_mutate <- str_replace_all(Vehicle_Message_History_raw_sample$Transit_Day,"-", "")
# 
# # add unique trip identifier:
# 
# Vehicle_Message_History_raw_sample$AdHocUniqueTripNumber <- str_c(Vehicle_Message_History_raw_sample$Inbound_Outbound,
#                                                                   Vehicle_Message_History_raw_sample$Transit_Day_for_mutate,
#                                                                   Vehicle_Message_History_raw_sample$Trip,
#                                                                   Vehicle_Message_History_raw_sample$Vehicle_ID,
#                                                                   Vehicle_Message_History_raw_sample$Run_Id)
# 
# # now get clock hour
# 
# Vehicle_Message_History_raw_sample$Clock_Hour <- str_sub(Vehicle_Message_History_raw_sample$Clock_Time, 1, 2)
# 
# # get seconds since midnight
# 
# Vehicle_Message_History_raw_sample$seconds_between_dates <- difftime(Vehicle_Message_History_raw_sample$Date,
#                                                                      Vehicle_Message_History_raw_sample$Transit_Day,
#                                                                      units = "secs")
# 
# # maybe just need to use GPS_Time format...
# 
# Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_raw_sample$Time.x,
#                                                                                Vehicle_Message_History_raw_sample$Date, 
#                                                                                units = "secs")
# 
# # add those two seconds to get seconds since (true) midnight
# 
# Vehicle_Message_History_raw_sample$seconds_since_midnight <- Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time +
#   Vehicle_Message_History_raw_sample$seconds_between_dates
# 
# # clean the format up (dplyr no like POSIXlt)
# 
# Vehicle_Message_History_raw_sample$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_raw_sample$seconds_since_midnight))
# 
# # now.... need to apply validation criteria to Vehicle_Message_History_raw_sample
# # to develop set of valid/invalid trips...
# 
# # get days in which vehicles had no APC data

zero_boarding_alighting_vehicles_VMH  <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  filter(Boardings == 0 | Alightings == 0)

zero_boarding_alighting_vehicles_VMH 


# remove those vehicles on those days

# Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID"))# %>%
# # filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

# Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles_APC, by = c("Transit_Day", "Vehicle_ID" = "MDCID"))# %>%
# filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
  anti_join(zero_boarding_alighting_vehicles_VMH, by = c("Transit_Day", "Vehicle_ID"))# %>%
# filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

# looks good! 
# now we need to identify vehicles with wacky APCs.

Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  arrange(Transit_Day) %>%
  View() # looks like 1994 and 1988 were still causing problems.

# make variable that is two standard deviations from the mean

library(magrittr)

boardings_three_sd_from_mean <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
                                   group_by(Transit_Day, Vehicle_ID) %>%
                                   summarise(Boardings = sum(Boards),
                                             Alightings =sum(Alights)) %$%
                                   sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %$%
     mean(Boardings))

# get vehicles that are three sd from mean

wacky_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_three_sd_from_mean)

# get summary of wacky vehicles

wacky_apc_vehicles %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boardings))

# build obvious outlier set (for Option 4 - step 1)

Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 1250 # this still holds for November...

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

# and then get vehicle days above 2 SDs above mean (for Option 4 - step 2)

boardings_three_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
                                                       group_by(Transit_Day, Vehicle_ID) %>%
                                                       summarise(Boardings = sum(Boards),
                                                                 Alightings = sum(Alights)) %>%
                                                       anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                       sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_three_sd_from_mean_outlier_set_removed)

# option 5 - (permutation of option 4) - remove outliers and then vehicles whose boardings/alightings difference are >5%, for vehicles above 2 sds above mean

Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 1250

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

boardings_three_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
                                                       group_by(Transit_Day, Vehicle_ID) %>%
                                                       summarise(Boardings = sum(Boards),
                                                                 Alightings = sum(Alights)) %>%
                                                       anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                       sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

high_end_wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  mutate(Percent_Difference = (Boardings - Alightings)/ ((Boardings + Alightings)/2))%>%
  filter(Boardings > obvious_outlier |
           (Boardings > boardings_three_sd_from_mean_outlier_set_removed) &
           Percent_Difference > 0.1 | 
           (Boardings > boardings_three_sd_from_mean_outlier_set_removed) &
           Percent_Difference < -0.1)

# do final comparison

View(high_end_wacky_apc_vehicles_reiterated )

Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  arrange(Transit_Day) %>%
  View() # so.... a couple of 1994 vehicles sneak through but....

# remove those vehicles -- Option 5 -- remove obvious outliers, then 
# remove vehicles surpassing 5% threshold to vehicle-days w/ high boardings

Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample %>%
  anti_join(zero_boarding_alighting_vehicles_VMH, by = c("Transit_Day", "Vehicle_ID")) %>%
  anti_join(high_end_wacky_apc_vehicles_reiterated , by = c("Transit_Day", "Vehicle_ID"))

# # now build dataset of invalid data -- Option 3 (see above) AND Option 4 (see above) AND Option 5
# 
Vehicle_Message_History_raw_sample_invalid <- Vehicle_Message_History_raw_sample %>%
  anti_join(Vehicle_Message_History_raw_sample_clean)

# now get stop data

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                       Database = "DW_IndyGo", 
                       Port = 1433)

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

DimStop <- DimStop_raw %>%
  filter(is.na(DeleteDate), !is.na(Latitude), ActiveInd == 1)

# check nrow before join

nrow(Vehicle_Message_History_raw_sample_clean ) == nrow(Vehicle_Message_History_raw_sample_clean %>%
  left_join(DimStop, by = c("Stop_Id" = "StopID"))) # good!

# OLD SCRIPT, new digs

Stop_total_boardings <- Vehicle_Message_History_raw_sample_clean %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  group_by(Direction, StopDesc) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  # filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Direction), desc(Latitude)) %>%
  # filter(!Direction == "SouthBound" & !StopDesc == "University Station NB") %>%
  select(Direction, Station = StopDesc, Boardings, Alightings) %>%
  formattable::formattable()  # still some bad data... but export this

Stop_average_boardings <- Vehicle_Message_History_raw_sample_clean %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  group_by(Direction, Service_Type, StopDesc, Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boards),
            Daily_Alightings = sum(Alights)) %>%
  group_by(Direction, StopDesc, Service_Type) %>%
  summarise(Average_Daily_Boardings = round(mean(Daily_Boardings)),
            Average_Daily_Alightings = round(mean(Daily_Alightings))) %>%
  # filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Direction), desc(Latitude)) %>%
  # filter(!Direction == "SouthBound" & !StopDesc == "University Station NB") %>%
  select(Direction, Station = StopDesc, Service_Type,
         Average_Daily_Boardings, Average_Daily_Alightings) %>%
  formattable::formattable() # export this

# # let's expand on the above for service type
# 
# Vehicle_Message_History_raw_sample_clean %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
#   mutate(Direction = ifelse(Inbound_Outbound == 0, "NB", "SB")) %>%
#   mutate(StopDesc = ifelse(StopDesc == "66th Station", 
#                            ifelse(Direction == "SB", 
#                                   "66th Station SB", "66th Station NB"),
#                            StopDesc)) %>%
#   group_by(StopDesc, Service_Type, Transit_Day) %>%
#   summarise(Daily_Boardings = sum(Boards),
#             Daily_Alightings = sum(Alights)) %>%
#   group_by(StopDesc, Service_Type)%>%
#   summarise(Average_Daily_Boardings = round(mean(Daily_Boardings)),
#             Average_Daily_Alightings = round(mean(Daily_Alightings))) %>%
#   # filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
#   arrange(desc(Latitude)) %>%
#   filter(StopDesc != "College Ave & 66th St") %>%
#   # ungroup() %>% # this makes it much more expensive operation...
#   select(Station = StopDesc, Service_Type, Average_Daily_Boardings) %>%
#   formattable::formattable() # be careful this doesn't filter out UOI NB in its entirity... (also, why 66th bottom of list?)

# same as above, but for PLATFORMS (or something like platforms..)

station_average_boardings <- Vehicle_Message_History_raw_sample_clean %>% 
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  # mutate(StopDesc = ifelse(StopDesc == "66th Station",
  #                          ifelse(Direction == "Southbound",
  #                                 "66th Station SB", "66th Station NB"),
  #                          StopDesc)) %>%
  mutate(StopDesc = ifelse(str_detect(StopDesc, "CTC"),  # this is the operative function
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-2),
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-3))) %>%
  mutate(StopDesc = ifelse(StopDesc == "66th Stat", "66th Station", StopDesc)) %>%
  group_by(StopDesc, Service_Type, Transit_Day) %>% # (but I lose lat data by the above)
  summarise(Daily_Boardings = sum(Boards),
            Daily_Alightings = sum(Alights)) %>%
  group_by(StopDesc, Service_Type) %>%
  summarise(Average_Daily_Boardings = round(mean(Daily_Boardings)),
            Average_Daily_Alightings = round(mean(Daily_Alightings))) %>%
  select(Station = StopDesc, Service_Type, Average_Daily_Boardings, Average_Daily_Alightings) %>%
  filter(!is.na(Station))  %>%
  formattable::formattable() 

station_average_boardings # export this

# write.csv(station_average_boardings, "201911 Station Average Boardings.csv", row.names = FALSE)

station_total_boardings <- Vehicle_Message_History_raw_sample_clean %>% 
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  mutate(StopDesc = ifelse(str_detect(StopDesc, "CTC"),  # this is the operative function
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-2),
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-3))) %>%
  mutate(StopDesc = ifelse(StopDesc == "66th Stat", "66th Station", StopDesc)) %>%
  group_by(StopDesc) %>% # (but I lose lat data by the above)
  summarise(Total_Boardings = sum(Boards),
            Total_Alightings = sum(Alights)) %>%
  select(Station = StopDesc, Total_Boardings, Total_Alightings) %>%
  filter(!is.na(Station))  %>%
  formattable::formattable() 

station_total_boardings # export this

# write.csv(station_total_boardings, "201911 Station Total Boardings.csv", row.names = FALSE)

# library(extrafont)
# library(ggthemes)

Vehicle_Message_History_raw_sample_clean %>% # graph it - boardings
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  group_by(Direction, StopDesc) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Direction), desc(Latitude)) %>%
  # filter(StopDesc != "College Ave & 66th St", 
  #        Direction != "SouthBound" && StopDesc != "University Station NB") %>%
  filter(!is.na(StopDesc)) %>%
  ungroup()%>% 
  # mutate(StopDesc = ifelse(StopDesc == "66th Station", 
  #                          ifelse(Direction == "Southbound", 
  #                                 "66th Station SB", "66th Station NB"),
  #                          StopDesc)) %>%
  mutate(StopDesc = factor(StopDesc, levels = StopDesc, ordered = TRUE)) %>% # so... 66th is problem
  # select(Direction, Station = StopDesc, Boardings, Alightings) %>%
  ggplot()+
  geom_bar(aes(x = reorder(StopDesc, desc(StopDesc)), y = Boardings), stat = "identity") +
  coord_flip()+
  facet_wrap(~Direction, scales='free_y') +
  labs(x = "", title = "Boardings by Red Line Station", subtitle = "January - February, 2020")

Vehicle_Message_History_raw_sample_clean %>% # graph it - alightings
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  group_by(Direction, StopDesc) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Direction), desc(Latitude)) %>%
  # filter(StopDesc != "College Ave & 66th St", 
  #        Direction != "SouthBound" && StopDesc != "University Station NB") %>%
  filter(!is.na(StopDesc)) %>%
  ungroup()%>% 
  # mutate(StopDesc = ifelse(StopDesc == "66th Station", 
  #                          ifelse(Direction == "Southbound", 
  #                                 "66th Station SB", "66th Station NB"),
  #                          StopDesc)) %>%
  mutate(StopDesc = factor(StopDesc, levels = StopDesc, ordered = TRUE)) %>% # so... 66th is problem
  # select(Direction, Station = StopDesc, Boardings, Alightings) %>%
  ggplot()+
  geom_bar(aes(x = reorder(StopDesc, desc(StopDesc)), y = Alightings), stat = "identity") +
  coord_flip()+
  facet_wrap(~Direction, scales='free_y') +
  labs(x = "", title = "Alightings by Red Line Station", subtitle = "January - February, 2020")

Vehicle_Message_History_raw_sample_clean %>% # graph it boardings AND alightings...
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  group_by(Direction, StopDesc) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights),
            Activity = sum(Boards) + sum(Alights)) %>%
  filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Direction), desc(Latitude)) %>%
  # filter(StopDesc != "College Ave & 66th St") %>%
  filter(!is.na(StopDesc)) %>%
  ungroup()%>% 
  # mutate(StopDesc = ifelse(StopDesc == "66th Station", 
  #                          ifelse(Direction == "Southbound", 
  #                                 "66th Station SB", "66th Station NB"),
  #                          StopDesc)) %>%
  mutate(StopDesc = factor(StopDesc, levels = StopDesc, ordered = TRUE)) %>% # so... 66th is problem
  # select(Direction, Station = StopDesc, Boardings, Alightings) %>%
  ggplot()+
  geom_bar(aes(x = reorder(StopDesc, desc(StopDesc)), y = Activity), stat = "identity") +
  coord_flip()+
  facet_wrap(~Direction, scales='free_y') +
  labs(x = "", title = "Boardings + Alightings by Red Line Station", subtitle = "January - February, 2020")

# export these data, too

write.csv(Stop_total_boardings, 
          "202001_202002_RL_Total_Boardings_by_Stop.csv", row.names = FALSE)
write.csv(Stop_average_boardings, # resend
          "202001_202002_RL_Average_Daily_Boardings_by_Stop_by_Service_Type_Revised.csv", row.names = FALSE)
write.csv(station_total_boardings,
          "202001_202002_RL_Total_Boardings_by_Station_Area.csv", row.names = FALSE)
write.csv(station_average_boardings, 
          "202001_202002_RL_Average_Daily_Boardings_by_Station_Area_by_Service_Type.csv", row.names = FALSE)

Vehicle_Message_History_raw_sample_clean %>%
  filter(Boards > 0 | Alights > 0) %>%
  select(Vehicle_ID, Time.x, Transit_Day, Route, Trip, Inbound_Outbound, 
         Onboard, Boards, Alights, Stop_Id, Service_Type, Latitude, Longitude) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc), by = c("Stop_Id" = "StopID")) %>%
  write.csv(., "202001_202002_Raw_APC_data.csv",
            row.names = FALSE)

# Vehicle_Message_History_raw_sample_clean %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
#   mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
#   group_by(Direction, StopDesc) %>%
#   summarise(Boardings = sum(Boards),
#             Alightings = sum(Alights)) %>%
#   filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
#   filter(StopDesc != "College Ave & 66th St", 
#          Direction != "SouthBound" && StopDesc != "University Station NB") %>%
#   mutate(StopDesc = ifelse(StopDesc == "66th Station", 
#                            ifelse(Direction == "Southbound", 
#                                   "66th Station SB", "66th Station NB"),
#                            StopDesc)) %>%
#   arrange(Direction, desc(Latitude)) %>%
#   mutate(Stop_Order = ifelse(Direction == "Northbound", n():1, 1:n())) %>%
#   select(Direction, Stop_Order,Station = StopDesc, Boardings, Alightings) %>%
#   write.csv("Boardings_and_Alightings_by_RL_Station_November_2019.csv",
#             row.names = FALSE)

# (also get unattributed stops and expanded stops)

Vehicle_Message_History_raw_sample_clean %>%
  group_by(Stop_Id) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  filter(Boardings != 0 ,  Alightings != 0, Boardings > 100) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc), by = c("Stop_Id" = "StopID")) %>%
  ungroup()%>%
  mutate(Rank = rank(desc(Boardings))) %>%
  select(StopDesc, Rank, Boardings, Alightings) %>%
  arrange(Rank) %>%
  formattable::formattable()

# let's cut the last three letters for every stop ending in "B",
# and then merge the results. 

# Also merge DTC
# Also figure out how to merge College and 66th.

Vehicle_Message_History_raw_sample_clean %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc), by = c("Stop_Id" = "StopID")) %>%
  mutate(Station_Name = ifelse(str_detect(StopDesc, "B$"), str_sub(StopDesc, end = -4), 
                               ifelse(str_detect(StopDesc, "^DTC"), str_sub(StopDesc, end = -3),
                                      StopDesc))) %>%
  group_by(Station_Name) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  filter(Boardings != 0 |  Alightings != 0, Boardings > 20) %>%
  mutate(Rank = rank(desc(Boardings))) %>%
  select(Station_Name, Rank, Boardings, Alightings) %>%
  arrange(Rank) %>%
  formattable::formattable()

# ridership per trip?

Vehicle_Message_History_raw_sample_clean %>%
  group_by(AdHocUniqueTripNumber) %>%
  summarise(Trip_Boardings = sum(Boards)) %>%
  arrange(desc(Trip_Boardings)) %>%
  View()

Vehicle_Message_History_raw_sample_clean %>%
  group_by(AdHocUniqueTripNumber) %>%
  summarise(Trip_Boardings = sum(Boards)) %>%
  arrange(desc(Trip_Boardings)) %>%
  ggplot()+
  geom_histogram(aes(x = Trip_Boardings), binwidth = 1)

# hm...... those zero-boarding trips are midly suspicious.

###

# lets get station ridership per day

Vehicle_Message_History_raw_sample_clean %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc), by = c("Stop_Id" = "StopID")) %>%
  group_by(StopDesc, Transit_Day) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  ungroup()%>%
  arrange(StopDesc, Transit_Day) %>%
  select(Stop = StopDesc, Date = Transit_Day, Boardings, Alightings) %>%
  formattable::formattable()

# let's map plain old ridership by day

Vehicle_Message_History_raw_sample_clean %>%
  group_by(Transit_Day) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  ungroup()%>%
  arrange(Transit_Day) %>%
  select(Date = Transit_Day, Boardings, Alightings) %>%
  ggplot()+
  geom_bar(aes(x = Date, y = Boardings), stat = "identity")

#### 4/24/20 Update ####

library(tidyverse)
library(timeDate)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

Vehicle_Message_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-08-31 03:00:00", Time < "2020-04-02 03:00:00",
         Route_Name == "90", Route == 90,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899, 
         Boards > 0 | Alights > 0) %>%
  select(Time, Route, Route_Name, Vehicle_ID, Avl_History_Id, Trip,
         Stop_Id, Boards, Alights, Inbound_Outbound) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-08-31 03:00:00", Time < "2020-04-02 03:00:00") %>%
  collect() # consider doing some semi_join or something 

### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample  %>%
  left_join(Vehicle_Avl_History_raw_sample, by = "Avl_History_Id")

# Filter by latitude to create Red Line set

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Latitude > 39.709468, Latitude < 39.877512)

# Filter by longitude to remove garage boardings, if any

# Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
#   filter(Longitude > -86.173321) # not sure if this is right...

# format dates in VMH

Vehicle_Message_History_raw_sample$Date <- as.Date(str_sub(Vehicle_Message_History_raw_sample$Time.x, 1, 10))

Vehicle_Message_History_raw_sample$Clock_Time <- str_sub(Vehicle_Message_History_raw_sample$Time.x, 12, 19)

Vehicle_Message_History_raw_sample$DateTest <- ifelse(Vehicle_Message_History_raw_sample$Clock_Time < 
                                                        "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_raw_sample$Transit_Day_Unix <- ifelse(Vehicle_Message_History_raw_sample$DateTest == 1,
                                                              lubridate::as_date(Vehicle_Message_History_raw_sample$Date - 1),
                                                              Vehicle_Message_History_raw_sample$Date)

# add two dates together

Vehicle_Message_History_raw_sample$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_raw_sample$Transit_Day <- Vehicle_Message_History_raw_sample$Epoch_Date + lubridate::days(Vehicle_Message_History_raw_sample$Transit_Day_Unix)

# just get February data

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Transit_Day >= "2019-09-01", Transit_Day < "2020-04-01")

# confirm dates

sort(unique(Vehicle_Message_History_raw_sample$Transit_Day))

# now set service types

# Vehicle_Message_History_raw_sample$Date <- as.Date(Vehicle_Message_History_raw_sample$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_raw_sample$Year <- lubridate::year(Vehicle_Message_History_raw_sample$Transit_Day)
Vehicle_Message_History_raw_sample$Month <- lubridate::month(Vehicle_Message_History_raw_sample$Transit_Day)
Vehicle_Message_History_raw_sample$Day <- lubridate::wday(Vehicle_Message_History_raw_sample$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_raw_sample_weekdays <- Vehicle_Message_History_raw_sample %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_raw_sample_saturday <- Vehicle_Message_History_raw_sample %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_raw_sample_sunday <- Vehicle_Message_History_raw_sample %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_raw_sample_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_raw_sample_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_raw_sample_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_raw_sample <- rbind(Vehicle_Message_History_raw_sample_weekdays, Vehicle_Message_History_raw_sample_sunday,
                                            Vehicle_Message_History_raw_sample_saturday)

# # mutate calendar day for identifier:
# 
# Vehicle_Message_History_raw_sample$Transit_Day_for_mutate <- str_replace_all(Vehicle_Message_History_raw_sample$Transit_Day,"-", "")
# 
# # add unique trip identifier:
# 
# Vehicle_Message_History_raw_sample$AdHocUniqueTripNumber <- str_c(Vehicle_Message_History_raw_sample$Inbound_Outbound,
#                                                                   Vehicle_Message_History_raw_sample$Transit_Day_for_mutate,
#                                                                   Vehicle_Message_History_raw_sample$Trip,
#                                                                   Vehicle_Message_History_raw_sample$Vehicle_ID,
#                                                                   Vehicle_Message_History_raw_sample$Run_Id)
# 
# # now get clock hour
# 
# Vehicle_Message_History_raw_sample$Clock_Hour <- str_sub(Vehicle_Message_History_raw_sample$Clock_Time, 1, 2)
# 
# # get seconds since midnight
# 
# Vehicle_Message_History_raw_sample$seconds_between_dates <- difftime(Vehicle_Message_History_raw_sample$Date,
#                                                                      Vehicle_Message_History_raw_sample$Transit_Day,
#                                                                      units = "secs")
# 
# # maybe just need to use GPS_Time format...
# 
# Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_raw_sample$Time.x,
#                                                                                Vehicle_Message_History_raw_sample$Date, 
#                                                                                units = "secs")
# 
# # add those two seconds to get seconds since (true) midnight
# 
# Vehicle_Message_History_raw_sample$seconds_since_midnight <- Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time +
#   Vehicle_Message_History_raw_sample$seconds_between_dates
# 
# # clean the format up (dplyr no like POSIXlt)
# 
# Vehicle_Message_History_raw_sample$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_raw_sample$seconds_since_midnight))
# 
# # now.... need to apply validation criteria to Vehicle_Message_History_raw_sample
# # to develop set of valid/invalid trips...
# 
# # get days in which vehicles had no APC data

zero_boarding_alighting_vehicles_VMH  <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  filter(Boardings == 0 | Alightings == 0)

zero_boarding_alighting_vehicles_VMH 


# remove those vehicles on those days

# Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID"))# %>%
# # filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

# Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles_APC, by = c("Transit_Day", "Vehicle_ID" = "MDCID"))# %>%
# filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
  anti_join(zero_boarding_alighting_vehicles_VMH, by = c("Transit_Day", "Vehicle_ID"))# %>%
# filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

# looks good! 
# now we need to identify vehicles with wacky APCs.

Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  arrange(Transit_Day) %>%
  View() # looks like 1994 and 1988 were still causing problems.

# make variable that is two standard deviations from the mean

library(magrittr)

boardings_three_sd_from_mean <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
                                   group_by(Transit_Day, Vehicle_ID) %>%
                                   summarise(Boardings = sum(Boards),
                                             Alightings =sum(Alights)) %$%
                                   sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %$%
     mean(Boardings))

# get vehicles that are three sd from mean

wacky_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_three_sd_from_mean)

# get summary of wacky vehicles

wacky_apc_vehicles %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boardings))

# build obvious outlier set (for Option 4 - step 1)

Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 1250 # this still holds for November...

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

# and then get vehicle days above 2 SDs above mean (for Option 4 - step 2)

boardings_three_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
                                                       group_by(Transit_Day, Vehicle_ID) %>%
                                                       summarise(Boardings = sum(Boards),
                                                                 Alightings = sum(Alights)) %>%
                                                       anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                       sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_three_sd_from_mean_outlier_set_removed)

# option 5 - (permutation of option 4) - remove outliers and then vehicles whose boardings/alightings difference are >5%, for vehicles above 2 sds above mean

Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 1250

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

boardings_three_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
                                                       group_by(Transit_Day, Vehicle_ID) %>%
                                                       summarise(Boardings = sum(Boards),
                                                                 Alightings = sum(Alights)) %>%
                                                       anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                       sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

high_end_wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  mutate(Percent_Difference = (Boardings - Alightings)/ ((Boardings + Alightings)/2))%>%
  filter(Boardings > obvious_outlier |
           (Boardings > boardings_three_sd_from_mean_outlier_set_removed) &
           Percent_Difference > 0.1 | 
           (Boardings > boardings_three_sd_from_mean_outlier_set_removed) &
           Percent_Difference < -0.1)

# do final comparison

View(high_end_wacky_apc_vehicles_reiterated )

Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  arrange(Transit_Day) %>%
  View() # so.... a couple of 1994 vehicles sneak through but....

# remove those vehicles -- Option 5 -- remove obvious outliers, then 
# remove vehicles surpassing 5% threshold to vehicle-days w/ high boardings

Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample %>%
  anti_join(zero_boarding_alighting_vehicles_VMH, by = c("Transit_Day", "Vehicle_ID")) %>%
  anti_join(high_end_wacky_apc_vehicles_reiterated , by = c("Transit_Day", "Vehicle_ID"))

# # now build dataset of invalid data -- Option 3 (see above) AND Option 4 (see above) AND Option 5
# 
Vehicle_Message_History_raw_sample_invalid <- Vehicle_Message_History_raw_sample %>%
  anti_join(Vehicle_Message_History_raw_sample_clean)

# do a check of the clean data to make sure no wacky vehilces snuck through

Vehicle_Message_History_raw_sample_clean %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boards = sum(Boards),
            Alightgs = sum(Alights)) %>%
  View()

# now get stop data

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                       Database = "DW_IndyGo", 
                       Port = 1433)

DimStop_raw <- tbl(con2, "DimStop") %>%
  collect()

DimStop <- DimStop_raw %>%
  filter(is.na(DeleteDate), !is.na(Latitude), ActiveInd == 1)

# check nrow before join

nrow(Vehicle_Message_History_raw_sample_clean ) == nrow(Vehicle_Message_History_raw_sample_clean %>%
                                                          left_join(DimStop, by = c("Stop_Id" = "StopID"))) # good!

# let's mutate for stations (... maybe)

# Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample_clean %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
#   mutate(Station_Area = ifelse(str_detect(StopDesc, "CTC"),  # this is the operative function
#                            str_sub(StopDesc, 1, end = str_length(StopDesc)-2),
#                            str_sub(StopDesc, 1, end = str_length(StopDesc)-3))) %>%
#   mutate(Station_Area = ifelse(Station_Area == "66th Stat", "66th Station", Station_Area))

# OLD SCRIPT, new digs

# Stop_total_boardings <- Vehicle_Message_History_raw_sample_clean %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
#   mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
#   group_by(Direction, StopDesc) %>%
#   summarise(Boardings = sum(Boards),
#             Alightings = sum(Alights)) %>%
#   # filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
#   arrange(desc(Direction), desc(Latitude)) %>%
#   # filter(!Direction == "SouthBound" & !StopDesc == "University Station NB") %>%
#   select(Direction, Station = StopDesc, Boardings, Alightings) %>%
#   formattable::formattable()  # still some bad data... but export this

Monthly_Stop_average_boardings <- Vehicle_Message_History_raw_sample_clean %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  mutate(Month = lubridate::month(Transit_Day, label = TRUE, abbr = TRUE)) %>%
  group_by(Direction, Service_Type, StopDesc, Month, Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boards),
            Daily_Alightings = sum(Alights)) %>%
  group_by(Direction, StopDesc, Service_Type, Month) %>%
  summarise(Average_Daily_Boardings = round(mean(Daily_Boardings)),
            Average_Daily_Alightings = round(mean(Daily_Alightings))) %>%
  # filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Direction), desc(Latitude)) %>%
  # filter(!Direction == "SouthBound" & !StopDesc == "University Station NB") %>%
  select(Month, Direction, Station = StopDesc, Service_Type,
         Average_Daily_Boardings, Average_Daily_Alightings) %>%
  formattable::formattable() # export this

Stop_average_boardings <- Vehicle_Message_History_raw_sample_clean %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  mutate(Month = lubridate::month(Transit_Day, label = TRUE, abbr = TRUE)) %>%
  group_by(Direction, StopDesc,Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boards),
            Daily_Alightings = sum(Alights)) %>%
  group_by(Direction, StopDesc) %>%
  summarise(Average_Daily_Boardings = round(mean(Daily_Boardings)),
            Average_Daily_Alightings = round(mean(Daily_Alightings))) %>%
  # filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Direction), desc(Latitude)) %>%
  # filter(!Direction == "SouthBound" & !StopDesc == "University Station NB") %>%
  select(Direction, Station = StopDesc, 
         Average_Daily_Boardings, Average_Daily_Alightings) %>%
  formattable::formattable() 

View(Stop_average_boardings)
# # let's expand on the above for service type
# 
# Vehicle_Message_History_raw_sample_clean %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
#   mutate(Direction = ifelse(Inbound_Outbound == 0, "NB", "SB")) %>%
#   mutate(StopDesc = ifelse(StopDesc == "66th Station", 
#                            ifelse(Direction == "SB", 
#                                   "66th Station SB", "66th Station NB"),
#                            StopDesc)) %>%
#   group_by(StopDesc, Service_Type, Transit_Day) %>%
#   summarise(Daily_Boardings = sum(Boards),
#             Daily_Alightings = sum(Alights)) %>%
#   group_by(StopDesc, Service_Type)%>%
#   summarise(Average_Daily_Boardings = round(mean(Daily_Boardings)),
#             Average_Daily_Alightings = round(mean(Daily_Alightings))) %>%
#   # filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
#   left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
#   arrange(desc(Latitude)) %>%
#   filter(StopDesc != "College Ave & 66th St") %>%
#   # ungroup() %>% # this makes it much more expensive operation...
#   select(Station = StopDesc, Service_Type, Average_Daily_Boardings) %>%
#   formattable::formattable() # be careful this doesn't filter out UOI NB in its entirity... (also, why 66th bottom of list?)

# same as above, but for PLATFORMS (or something like platforms..)

station_average_boardings <- Vehicle_Message_History_raw_sample_clean %>% 
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  # mutate(StopDesc = ifelse(StopDesc == "66th Station",
  #                          ifelse(Direction == "Southbound",
  #                                 "66th Station SB", "66th Station NB"),
  #                          StopDesc)) %>%
  mutate(StopDesc = ifelse(str_detect(StopDesc, "CTC"),  # this is the operative function
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-2),
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-3))) %>%
  mutate(StopDesc = ifelse(StopDesc == "66th Stat", "66th Station", StopDesc)) %>%
  group_by(StopDesc, Service_Type, Transit_Day) %>% # (but I lose lat data by the above)
  summarise(Daily_Boardings = sum(Boards),
            Daily_Alightings = sum(Alights)) %>%
  group_by(StopDesc, Service_Type) %>%
  summarise(Average_Daily_Boardings = round(mean(Daily_Boardings)),
            Average_Daily_Alightings = round(mean(Daily_Alightings))) %>%
  select(Station = StopDesc, Service_Type, Average_Daily_Boardings, Average_Daily_Alightings) %>%
  filter(!is.na(Station))  %>%
  formattable::formattable() 

station_average_boardings # export this

Station_average_boardings <- Vehicle_Message_History_raw_sample_clean %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  mutate(Month = lubridate::month(Transit_Day, label = TRUE, abbr = TRUE)) %>%
  mutate(StopDesc = ifelse(str_detect(StopDesc, "CTC"),  # this is the operative function
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-2),
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-3))) %>%
  mutate(StopDesc = ifelse(StopDesc == "66th Stat", "66th Station", StopDesc)) %>%
  group_by(StopDesc,Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boards),
            Daily_Alightings = sum(Alights)) %>%
  group_by(StopDesc) %>%
  summarise(Average_Daily_Boardings = round(mean(Daily_Boardings)),
            Average_Daily_Alightings = round(mean(Daily_Alightings))) %>%
  # filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Average_Daily_Boardings)) %>%
  # filter(!Direction == "SouthBound" & !StopDesc == "University Station NB") %>%
  select(Station = StopDesc, 
         Average_Daily_Boardings, Average_Daily_Alightings) %>%
  formattable::formattable() 

View(Station_average_boardings) # this is it.

# write.csv(station_average_boardings, "201911 Station Average Boardings.csv", row.names = FALSE)

station_total_boardings <- Vehicle_Message_History_raw_sample_clean %>% 
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  mutate(StopDesc = ifelse(str_detect(StopDesc, "CTC"),  # this is the operative function
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-2),
                           str_sub(StopDesc, 1, end = str_length(StopDesc)-3))) %>%
  mutate(StopDesc = ifelse(StopDesc == "66th Stat", "66th Station", StopDesc)) %>%
  group_by(StopDesc) %>% # (but I lose lat data by the above)
  summarise(Total_Boardings = sum(Boards),
            Total_Alightings = sum(Alights)) %>%
  select(Station = StopDesc, Total_Boardings, Total_Alightings) %>%
  filter(!is.na(Station))  %>%
  formattable::formattable() 

station_total_boardings # export this



# write.csv(station_total_boardings, "201911 Station Total Boardings.csv", row.names = FALSE)

# library(extrafont)
# library(ggthemes)

Vehicle_Message_History_raw_sample_clean %>% # graph it - boardings
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("Stop_Id" = "StopID")) %>%
  mutate(Direction = ifelse(Inbound_Outbound == 0, "Northbound", "Southbound")) %>%
  group_by(Direction, StopDesc, Month) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  filter(Boardings != 0 ,  Alightings != 0, Boardings > 20) %>%
  left_join(select(filter(DimStop_raw, ActiveInd == 1, PublicInd == 1, StopActive == 1), StopID, StopDesc, Latitude), by = c("StopDesc")) %>%
  arrange(desc(Direction), desc(Latitude)) %>%
  # filter(StopDesc != "College Ave & 66th St", 
  #        Direction != "SouthBound" && StopDesc != "University Station NB") %>%
  filter(!is.na(StopDesc)) %>%
  ungroup()%>% 
  # mutate(StopDesc = ifelse(StopDesc == "66th Station", 
  #                          ifelse(Direction == "Southbound", 
  #                                 "66th Station SB", "66th Station NB"),
  #                          StopDesc)) %>%
  # mutate(StopDesc = factor(StopDesc, levels = StopDesc, ordered = TRUE)) %>% # so... 66th is problem
  # select(Direction, Station = StopDesc, Boardings, Alightings) %>%
  ggplot()+
  geom_bar(aes(x = reorder(StopDesc, Boardings), y = Boardings), stat = "identity") +
  coord_flip()+
  facet_wrap(~Month, scales='free_y') +
  labs(x = "", title = "Boardings by Red Line Station", subtitle = "September, 2019 - March, 2020")

#### 5/1/20 Update ####

recent_GFI <- read.table(
  file = "EVENT SUMMARY (ROUTESUM) REPORT (MARCH 1, 2020 - APRIL 25, 2020).txt",
  skip = 5,
  header = TRUE,
  sep = ",",
  nrow = 1824)

### --- data cleaning --- ###

# clean GFI data

recent_GFI <- recent_GFI %>%
  select(Date, Route, Ridership)

# check out routes..

sort(unique(recent_GFI$Route))

# filter out routes..

recent_GFI <- recent_GFI %>%
  filter(Route %in% c(2,3,4,5,6,8,10,11,12,13,14,15,16,18,
                      19,21,24,25,26,28,30,
                      31,34,37,38,39,55,86,87))

# get a quick confirmation that the data match

recent_GFI %>%
  group_by(Date) %>%
  summarise(Daily_Ridership = sum(Ridership)) %>%
  View() # looks good!

# convert Date factor to real Date

recent_GFI$Date <- as.Date(recent_GFI$Date, format = "%m/%d/%Y")

str(recent_GFI) # looks good!

# split by date segment.

First_week <- recent_GFI %>%
  filter(Date >= "2020-03-01", Date <= "2020-03-14") %>%
  group_by(Route) %>%
  summarise(Average_Daily_Ridership = round(mean(Ridership)))%>%
  mutate(Route = as.integer(str_trim(Route, "both")))%>%
  arrange(Route)

Second_week <- recent_GFI %>%
  filter(Date >= "2020-03-29", Date <= "2020-04-25") %>%
  group_by(Route) %>%
  summarise(Average_Daily_Ridership = round(mean(Ridership)))%>%
  mutate(Route = as.integer(str_trim(Route, "both"))) %>%
  arrange(Route)

# now get, merge Red Line data

recent_data_BRT <- data.frame(Route = 90, Ridership = as.numeric(as.character(quote(c(   4799 
,4954 ,4842 ,4870 ,4362 ,3334 ,2204 ,4270 ,4266 
,4584 ,4637 ,3947 ,2018 ,1847 ,3299 ,3067 ,2712 
,2707 ,2638 ,1818 ,1289 ,2536 ,2367 ,2194 ,2267
,2273 ,1584,1317 ,2137,2065 ,2412,2007 ,2513
,1771 ,1509,2265,2306,2288,2148,2440,1866,1297,2013,2112
,2090,2167,2045,1925,1483,2328,2166,2335,1919,2302,1965,1414,2328,2383,2270,2291
)))[-1] 
),
Date = seq.Date(from = as.Date("2020-03-01"), to = as.Date("2020-04-29"), by = "day"))

View(recent_data_BRT)

First_week_BRT <- recent_data_BRT %>%
  filter(Date >= "2020-03-01", Date <= "2020-03-14") %>%
  group_by(Route) %>%
  summarise(Average_Daily_Ridership = round(mean(Ridership)))

Second_week_BRT <- recent_data_BRT %>%
  filter(Date >= "2020-03-29", Date <= "2020-04-25") %>%
  group_by(Route) %>%
  summarise(Average_Daily_Ridership = round(mean(Ridership)))
  
# bind to local

First_week <- rbind(First_week, First_week_BRT)
Second_week <- rbind(Second_week, Second_week_BRT)

# merge

Combined <- left_join(First_week, Second_week, by = "Route") %>%
  select(Route,
         "Average Daily Ridership \nMarch 1 - March 14" = Average_Daily_Ridership.x,
         "Average Daily Ridership \nMarch 29 - April 25" = Average_Daily_Ridership.y)

#### 5/18/20 Update ####

# 5/18/20 - Service Planning wants average weekday boardings for stops 11570, 11540,
#           maybe 52097 Westfield/College WB FS and IB & OB at Westfield and College.
#           Do 9/2019 to 3/2020 and 3/2020 to present.

# let's get csv of daily boardings at those two stops. Then cut it up.

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_stops <- tbl(con, "Apc_Data") %>%
  filter(Stop %in% c(11570, 11540), 
         GPS_Time >= "2019-09-01") %>%
  collect()

Apc_stops$Date <- as.Date(str_sub(Apc_stops$GPS_Time, 1, 10))

Apc_stops$Clock_Time <- str_sub(Apc_stops$GPS_Time, 12, 19)

Apc_stops$DateTest <- ifelse(Apc_stops$Clock_Time < 
                             "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_stops$Transit_Day_Unix <- ifelse(Apc_stops$DateTest == 1,
                                   lubridate::as_date(Apc_stops$Date - 1),
                                   Apc_stops$Date)

# add two dates together

Apc_stops$Epoch_Date <- as.Date("1970-01-01")

Apc_stops$Transit_Day <- Apc_stops$Epoch_Date + lubridate::days(Apc_stops$Transit_Day_Unix)

# set weekdays

Apc_stops$Year <- lubridate::year(Apc_stops$Transit_Day)
Apc_stops$Month <- lubridate::month(Apc_stops$Transit_Day)
Apc_stops$Day <- lubridate::wday(Apc_stops$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

library(timeDate)

holidays_sunday <- holiday(2000:2019, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2019, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Apc_stops_weekdays <- Apc_stops %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Apc_stops_saturday <- Apc_stops %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Apc_stops_sunday <- Apc_stops %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Apc_stops_sunday$Service_Type <- "Sunday"
Apc_stops_saturday$Service_Type <- "Saturday"
Apc_stops_weekdays$Service_Type <- "Weekday"

# rebind

Apc_stops <- rbind(Apc_stops_weekdays, Apc_stops_sunday,
                                               Apc_stops_saturday)

# now summarise

Apc_stops_weekdays %>%
  group_by(Transit_Day, Stop) %>%
  summarise(Total_Boardings =sum(Boarding))

Apc_stops %>%
  filter(Service_Type == "Weekday",
        Transit_Day >= "2019-09-02",
        Transit_Day <= "2020-03-01") %>%
  mutate(Stop = as.character(Stop)) %>%
  group_by(Transit_Day, Stop) %>%
  summarise(Total_Boardings =sum(Boarding)) %>%
  group_by(Stop) %>%
  summarise(Average_Weekday_Boardings = mean(Total_Boardings))

Apc_stops %>%
  filter(Service_Type == "Weekday",
         Transit_Day >= "2019-03-02" ) %>%
  mutate(Stop = as.character(Stop)) %>%
  group_by(Transit_Day, Stop) %>%
  summarise(Total_Boardings =sum(Boarding)) %>%
  group_by(Stop) %>%
  summarise(Average_Weekday_Boardings = mean(Total_Boardings))

# prepare full file for export

Apc_stops_weekdays %>%
  group_by(Transit_Day, Stop) %>%
  summarise(Total_Boardings =sum(Boarding)) %>%
  View()

# Apc_stops_weekdays %>%
#   group_by(Transit_Day, Stop) %>%
#   summarise(Total_Boardings =sum(Boarding)) %>%
#   write.csv(., "Weekday_Boardings_11570_11540_090119_051520.csv",
#             row.names = FALSE)

# also get these:

# 52097 – Westfield/Broadway WB NS A
# 52018 – Westfield/Broadway EB NSMB F
# 52851 – Westfield/Central WB NS E
# 52850 – Westfield/Central EB FS E

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_stops <- tbl(con, "Apc_Data") %>%
  filter(Stop %in% c(52097, 52018, 52851, 52850), 
         GPS_Time >= "2019-09-01") %>%
  collect()

Apc_stops$Date <- as.Date(str_sub(Apc_stops$GPS_Time, 1, 10))

Apc_stops$Clock_Time <- str_sub(Apc_stops$GPS_Time, 12, 19)

Apc_stops$DateTest <- ifelse(Apc_stops$Clock_Time < 
                               "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_stops$Transit_Day_Unix <- ifelse(Apc_stops$DateTest == 1,
                                     lubridate::as_date(Apc_stops$Date - 1),
                                     Apc_stops$Date)

# add two dates together

Apc_stops$Epoch_Date <- as.Date("1970-01-01")

Apc_stops$Transit_Day <- Apc_stops$Epoch_Date + lubridate::days(Apc_stops$Transit_Day_Unix)

# set weekdays

Apc_stops$Year <- lubridate::year(Apc_stops$Transit_Day)
Apc_stops$Month <- lubridate::month(Apc_stops$Transit_Day)
Apc_stops$Day <- lubridate::wday(Apc_stops$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

library(timeDate)

holidays_sunday <- holiday(2000:2019, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2019, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Apc_stops_weekdays <- Apc_stops %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Apc_stops_saturday <- Apc_stops %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Apc_stops_sunday <- Apc_stops %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Apc_stops_sunday$Service_Type <- "Sunday"
Apc_stops_saturday$Service_Type <- "Saturday"
Apc_stops_weekdays$Service_Type <- "Weekday"

# rebind

Apc_stops <- rbind(Apc_stops_weekdays, Apc_stops_sunday,
                   Apc_stops_saturday)

# now summarise

Apc_stops_weekdays %>%
  group_by(Transit_Day, Stop) %>%
  summarise(Total_Boardings =sum(Boarding))

Apc_stops %>%
  filter(Service_Type == "Weekday",
         Transit_Day >= "2019-09-02",
         Transit_Day <= "2020-03-01") %>%
  mutate(Stop = as.character(Stop)) %>%
  group_by(Transit_Day, Stop) %>%
  summarise(Total_Boardings =sum(Boarding)) %>%
  group_by(Stop) %>%
  summarise(Average_Weekday_Boardings = mean(Total_Boardings))

Apc_stops %>%
  filter(Service_Type == "Weekday",
         Transit_Day >= "2019-03-02" ) %>%
  mutate(Stop = as.character(Stop)) %>%
  group_by(Transit_Day, Stop) %>%
  summarise(Total_Boardings =sum(Boarding)) %>%
  group_by(Stop) %>%
  summarise(Average_Weekday_Boardings = mean(Total_Boardings))

#### 5/26/20 ####

# box plot of boardings per hour for route 901

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_901 <- tbl(con, "Apc_Data") %>%
  filter(MDCID >= 1950, MDCID < 2000 | MDCID == 1899, 
         GPS_Time >= "2020-04-03",
         Route == 901) %>%
  collect()


Apc_901$Date <- as.Date(str_sub(Apc_901$GPS_Time, 1, 10))

Apc_901$Clock_Time <- str_sub(Apc_901$GPS_Time, 12, 19)

Apc_901$DateTest <- ifelse(Apc_901$Clock_Time < 
                             "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_901$Transit_Day_Unix <- ifelse(Apc_901$DateTest == 1,
                                   lubridate::as_date(Apc_901$Date - 1),
                                   Apc_901$Date)

# add two dates together

Apc_901$Epoch_Date <- as.Date("1970-01-01")

Apc_901$Transit_Day <- Apc_901$Epoch_Date + lubridate::days(Apc_901$Transit_Day_Unix)

# get data since May 4th

Apc_901 <- Apc_901 %>%
  filter(Transit_Day >= "2020-05-04")

# get hour of day

Apc_901$Hour_of_Day <- str_sub(Apc_901$Clock_Time, 1, 2)

# now set service types

Apc_901$Year <- lubridate::year(Apc_901$Transit_Day)
Apc_901$Month <- lubridate::month(Apc_901$Transit_Day)
Apc_901$Day <- lubridate::wday(Apc_901$Transit_Day, label=TRUE)

# set holiday variables

library(timeDate)

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Apc_901_weekdays <- Apc_901 %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Apc_901_saturday <- Apc_901 %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Apc_901_sunday <- Apc_901 %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Apc_901_sunday$Service_Type <- "Sunday"
Apc_901_saturday$Service_Type <- "Saturday"
Apc_901_weekdays$Service_Type <- "Weekday"

# rebind

Apc_901 <- rbind(Apc_901_weekdays, Apc_901_sunday,
                                               Apc_901_saturday)
# transform

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  View()

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  ggplot() +
  geom_boxplot(aes(y = Boardings, x = Hour_of_Day)) +
  facet_wrap(~Service_Type)

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Min = min(Boardings),
            Max = max(Boardings),
            Avg = mean(Boardings),
            Median = median(Boardings)) %>%
  View()

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Min = min(Boardings),
            Max = max(Boardings),
            Avg = mean(Boardings),
            Median = median(Boardings)) %>%
  View()

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Median_Boardings = median(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Median_Boardings),
               stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Median Boardings", subtitle = "Per APCs, since May 4th")

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Mean_Boardings = mean(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Mean_Boardings),
           stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Average Boardings", subtitle = "Per APCs, since May 4th")

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Max_Boardings = max(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Max_Boardings),
           stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Max Boardings", subtitle = "Per APCs, since May 4th")

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Max_Boardings = max(Boardings)) %>%
  View()

Apc_901 %>%
  filter(Stop != 0) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  ggplot() +
  geom_boxplot(aes(y = Boardings, x = Hour_of_Day)) +
  facet_wrap(~Service_Type) +
  labs(title = "Route 901 Boardings - Boxplots", 
       subtitle = "Outliers, Medians, and Interquartile Ranges",
       caption = "Per APCs, since May 4th")

# what do daily boardings look like?

Apc_901 %>%
  group_by(Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boarding)) %>%
  View()

# what is up with those 01 hour boardings?-- maybe at garage?

# quick--review how many not at stops

Apc_901 %>%
  group_by(Stop) %>%
  summarise(Boardings = sum(Boarding)) %>%
  View()

# how much of the data is represetned by 0 stops ids?

library(magrittr)

Apc_901 %>%
  filter(Stop != 0) %$%
  sum(Boarding)

Apc_901 %>%
  filter(Stop == 0) %$%
  sum(Boarding) # Ouch! about 25 percent of boardings are unregistered...

# let's run the above but witout removing those stops...

Apc_901 %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Median_Boardings = median(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Median_Boardings),
           stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Median Boardings", subtitle = "Per APCs, since May 4th")

Apc_901 %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Mean_Boardings = mean(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Mean_Boardings),
           stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Average Boardings", subtitle = "Per APCs, since May 4th")

Apc_901 %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Max_Boardings = max(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Max_Boardings),
           stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Max Boardings", subtitle = "Per APCs, since May 4th")

Apc_901 %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Max_Boardings = max(Boardings)) %>%
  View()

Apc_901 %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boarding)) %>%
  ggplot() +
  geom_boxplot(aes(y = Boardings, x = Hour_of_Day)) +
  facet_wrap(~Service_Type) +
  labs(title = "Route 901 Boardings - Boxplots", 
       subtitle = "Outliers, Medians, and Interquartile Ranges",
       caption = "Per APCs, since May 4th")

# Apc_901 %>%
#   group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
#   summarise(Boardings = sum(Boarding)) %>%
#   ggplot()+
#   geom_violin(aes(y = Boardings, x = Hour_of_Day))  +
#   facet_wrap(~Service_Type) # nah

# Apc_901 %>%
#   group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
#   summarise(Boardings = sum(Boarding)) %>%
# ggplot(
#        aes(x=Hour_of_Day, ymin=min, lower=min,fill=Hour_of_Day ,
#            middle=`med`, upper=max, ymax=max)) +
#   geom_boxplot(stat="identity")


# (should probably do a geographic check...)

# let's do the same as above but for VMH..


con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

Vehicle_Message_History_901 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2020-05-03 03:00:00",
         Route_Name == "901", Route == 901,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899, 
         Boards > 0) %>%
  select(Time, Route, Route_Name, Vehicle_ID, Avl_History_Id, Trip,
         Stop_Id, Boards, Alights, Inbound_Outbound) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_901 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2020-05-03 03:00:00") %>%
  collect() # consider doing some semi_join or something 

### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_901 <- Vehicle_Message_History_901  %>%
  left_join(Vehicle_Avl_History_901, by = "Avl_History_Id")

# Filter by latitude to create Red Line set (NOT APPLICABLE HERE)

# Vehicle_Message_History_901 <- Vehicle_Message_History_901 %>%
#   filter(Latitude > 39.709468, Latitude < 39.877512)

# Filter by longitude to remove garage boardings, if any

garage_n <- nrow(Vehicle_Message_History_901 )
garage_n
Vehicle_Message_History_901  <- Vehicle_Message_History_901  %>%
  filter(Longitude > -86.173321)

no_garage_n <- nrow(Vehicle_Message_History_901 )
no_garage_n
# format dates in VMH

Vehicle_Message_History_901$Date <- as.Date(str_sub(Vehicle_Message_History_901$Time.x, 1, 10))

Vehicle_Message_History_901$Clock_Time <- str_sub(Vehicle_Message_History_901$Time.x, 12, 19)

Vehicle_Message_History_901$DateTest <- ifelse(Vehicle_Message_History_901$Clock_Time < 
                                                        "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_901$Transit_Day_Unix <- ifelse(Vehicle_Message_History_901$DateTest == 1,
                                                              lubridate::as_date(Vehicle_Message_History_901$Date - 1),
                                                              Vehicle_Message_History_901$Date)

# add two dates together

Vehicle_Message_History_901$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_901$Transit_Day <- Vehicle_Message_History_901$Epoch_Date + lubridate::days(Vehicle_Message_History_901$Transit_Day_Unix)

# just get May data

Vehicle_Message_History_901 <- Vehicle_Message_History_901 %>%
  filter(Transit_Day >= "2020-05-04")

# confirm dates

sort(unique(Vehicle_Message_History_901$Transit_Day))

# now set service types

# Vehicle_Message_History_901$Date <- as.Date(Vehicle_Message_History_901$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_901$Year <- lubridate::year(Vehicle_Message_History_901$Transit_Day)
Vehicle_Message_History_901$Month <- lubridate::month(Vehicle_Message_History_901$Transit_Day)
Vehicle_Message_History_901$Day <- lubridate::wday(Vehicle_Message_History_901$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_901_weekdays <- Vehicle_Message_History_901 %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_901_saturday <- Vehicle_Message_History_901 %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_901_sunday <- Vehicle_Message_History_901 %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_901_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_901_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_901_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_901 <- rbind(Vehicle_Message_History_901_weekdays, Vehicle_Message_History_901_sunday,
                                            Vehicle_Message_History_901_saturday)

# get hour of day

Vehicle_Message_History_901$Hour_of_Day <- str_sub(Vehicle_Message_History_901$Clock_Time, 1, 2)

# now run charts

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Min = min(Boardings),
            Max = max(Boardings),
            Avg = mean(Boardings),
            Median = median(Boardings)) %>%
  View()

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boards)) %>%
  View()

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Median_Boardings = median(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Median_Boardings),
           stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Median Boardings", subtitle = "Per APCs, since May 4th")

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Mean_Boardings = mean(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Mean_Boardings),
           stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Average Boardings", subtitle = "Per APCs, since May 4th")

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Max_Boardings = max(Boardings)) %>%
  ggplot()+
  geom_bar(aes(x =Hour_of_Day, y = Max_Boardings),
           stat = "identity") +
  facet_wrap(~ Service_Type) +
  labs(title = "Route 901 Max Boardings", subtitle = "Per APCs, since May 4th")

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  group_by(Service_Type, Hour_of_Day) %>%
  summarise(Max_Boardings = max(Boardings)) %>%
  View()

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  ggplot() +
  geom_boxplot(aes(y = Boardings, x = Hour_of_Day)) +
  facet_wrap(~Service_Type) +
  labs(title = "Route 901 Boardings - Boxplots", 
       subtitle = "Outliers, Medians, and Interquartile Ranges",
       caption = "Per APCs, since May 4th")

# what is up with Sunday? review.

Vehicle_Message_History_901 %>%
  filter(Service_Type == "Sunday") %>%
  View() # ah ha! most of these boardings are taking place at 14135... which is a non-public entry!

# need to remove these from the dataset...(done)

# 5/28 AV wants to know when was that day that had 18 boardings?

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  View()

# let's also add points to the boxplot

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  ggplot() +
  geom_boxplot(aes(y = Boardings, x = Hour_of_Day)) +
  geom_point(position = "jitter", aes(x = Hour_of_Day, y = Boardings,
                                      alpha = .5)) +
  facet_wrap(~Service_Type) +
  labs(title = "Route 901 Boardings - Boxplots", 
       subtitle = "Outliers, Medians, and Interquartile Ranges",
       caption = "Per APCs, since May 4th") # eh... doesn't look good.

# let's just make scatter plot.

set_breaks = function(limits) {
  seq(limits[1], limits[2], by = 1)
}

Vehicle_Message_History_901 %>%
  filter(Stop_Id != 14135) %>%
  group_by(Service_Type, Transit_Day, Hour_of_Day) %>%
  summarise(Boardings = sum(Boards)) %>%
  ggplot() +
  geom_count(aes(x = Hour_of_Day, y = Boardings, 
                 color = ..n.., size = ..n..),
             show.legend = TRUE) +
  scale_y_continuous(breaks = 1:18)+
  facet_wrap(~Service_Type) +
  labs(title = "Route 901 Boardings Per Hour", 
       subtitle = "n = number of days with corresponding number of boardings",
       caption = "Per APCs, since May 4th") +
  guides(color = 'legend') +
  scale_color_continuous(breaks = set_breaks) +
  scale_size_continuous(breaks = set_breaks)

#### 5/27/20 ####

# Get highest ridership day on RL since Jan 1.

# Potential issues:
# 1) Possibly some bad APCs
# 2) Possible 90/901/902 issues prior to split


con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "Steve_Temp", Port = 1433)

Apc_since_Jan_1_2020 <- tbl(con, "Apc_Data") %>%
  filter(MDCID >= 1950, MDCID < 2000 | MDCID == 1899, 
         GPS_Time >= "2019-12-31",
         Route == 90) %>%
  collect()


Apc_since_Jan_1_2020$Date <- as.Date(str_sub(Apc_since_Jan_1_2020$GPS_Time, 1, 10))

Apc_since_Jan_1_2020$Clock_Time <- str_sub(Apc_since_Jan_1_2020$GPS_Time, 12, 19)

Apc_since_Jan_1_2020$DateTest <- ifelse(Apc_since_Jan_1_2020$Clock_Time < 
                             "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Apc_since_Jan_1_2020$Transit_Day_Unix <- ifelse(Apc_since_Jan_1_2020$DateTest == 1,
                                   lubridate::as_date(Apc_since_Jan_1_2020$Date - 1),
                                   Apc_since_Jan_1_2020$Date)

# add two dates together

Apc_since_Jan_1_2020$Epoch_Date <- as.Date("1970-01-01")

Apc_since_Jan_1_2020$Transit_Day <- Apc_since_Jan_1_2020$Epoch_Date + lubridate::days(Apc_since_Jan_1_2020$Transit_Day_Unix)

# review vehicles per day

Apc_since_Jan_1_2020 %>%
  group_by(MDCID, Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boarding)) %>%
  arrange(desc(Daily_Boardings)) %>%
  View()

# look at 1994 and 1988 by day to see when they got better

Apc_since_Jan_1_2020 %>%
  filter(MDCID %in% c(1994, 1988))  %>%
  group_by(MDCID, Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boarding)) %>%
  arrange(desc(Transit_Day)) %>%
  View() # 2/13 seems to be the last bad day.

bad_data_set <- Apc_since_Jan_1_2020 %>%
  filter(MDCID %in% c(1994, 1988),
         Transit_Day <= "2020-02-13")  

Apc_since_Jan_1_2020_clean <- Apc_since_Jan_1_2020 %>%
  anti_join(bad_data_set)

# now get ridership by day

Apc_since_Jan_1_2020_clean %>%
  group_by(Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boarding)) %>%
  arrange(desc(Daily_Boardings)) %>%
  View() #shhiiittttt.. not segregated...

# let's just do RL stops???? (eh.....)

# let's see when 901/902 came into being...

Apc_901_902_since_Jan_1_2020 <- tbl(con, "Apc_Data") %>%
  filter(MDCID >= 1950, MDCID < 2000 | MDCID == 1899, 
         GPS_Time >= "2019-12-31",
         Route == 901 | Route == 902) %>%
  collect()            

# so split happend on 19th..

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

Vehicle_Message_History_90_since_jan <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-12-31 03:00:00",
         Route == 90,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899, 
         Boards > 0) %>%
  select(Time, Route, Route_Name, Vehicle_ID, Avl_History_Id, Trip,
         Stop_Id, Boards, Alights, Inbound_Outbound) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_sample_1 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-12-31 03:00:00", Time < "2020-02-01 03:00:00") %>%
  collect() # consider doing some semi_join or something 

Vehicle_Avl_History_raw_sample_2 <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2020-02-01 03:00:00", Time < "2020-03-20 03:00:00") %>%
  collect()
### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_90_since_jan  <- Vehicle_Message_History_90_since_jan  %>%
  left_join(Vehicle_Avl_History_raw_sample_1, by = "Avl_History_Id")%>%
  left_join(Vehicle_Avl_History_raw_sample_2, by = "Avl_History_Id")

# now clean NA set

Vehicle_Message_History_90_since_jan <- Vehicle_Message_History_90_since_jan %>% 
  mutate_at(vars(Latitude.x, Latitude.y, Longitude.x, Longitude.y), coalesce, 0)

Vehicle_Message_History_90_since_jan$Latitude <- Vehicle_Message_History_90_since_jan$Latitude.x +Vehicle_Message_History_90_since_jan$Latitude.y
Vehicle_Message_History_90_since_jan$Longitude <- Vehicle_Message_History_90_since_jan$Longitude.x + Vehicle_Message_History_90_since_jan$Longitude.y

# check to make sure no duplicate records... at some point...because of time crossover

nrow(Vehicle_Message_History_90_since_jan)
sum(table(unique(Vehicle_Message_History_90_since_jan$Avl_History_Id))) # great!

# Filter by latitude to create Red Line set

Vehicle_Message_History_90_since_jan <- Vehicle_Message_History_90_since_jan %>%
  filter(Latitude > 39.709468, Latitude < 39.877512) 

# filter by long to remove garage boardings

Vehicle_Message_History_90_since_jan <- Vehicle_Message_History_90_since_jan %>%
  filter(Longitude > -86.173321)

# Filter by longitude to remove garage boardings, if any

# Vehicle_Message_History_90_since_jan <- Vehicle_Message_History_90_since_jan %>%
#   filter(Longitude > -86.173321) # not sure if this is right...

# format dates in VMH

Vehicle_Message_History_90_since_jan$Date <- as.Date(str_sub(Vehicle_Message_History_90_since_jan$Time.x, 1, 10))

Vehicle_Message_History_90_since_jan$Clock_Time <- str_sub(Vehicle_Message_History_90_since_jan$Time.x, 12, 19)

Vehicle_Message_History_90_since_jan$DateTest <- ifelse(Vehicle_Message_History_90_since_jan$Clock_Time < 
                                                 "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_90_since_jan$Transit_Day_Unix <- ifelse(Vehicle_Message_History_90_since_jan$DateTest == 1,
                                                       lubridate::as_date(Vehicle_Message_History_90_since_jan$Date - 1),
                                                       Vehicle_Message_History_90_since_jan$Date)

# add two dates together

Vehicle_Message_History_90_since_jan$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_90_since_jan$Transit_Day <- Vehicle_Message_History_90_since_jan$Epoch_Date + lubridate::days(Vehicle_Message_History_90_since_jan$Transit_Day_Unix)

# remove bad data

bad_data_set_2 <- Vehicle_Message_History_90_since_jan %>%
  filter(Vehicle_ID %in% c(1994, 1988),
         Transit_Day <= "2020-02-13")  

Vehicle_Message_History_90_since_jan_clean <- Vehicle_Message_History_90_since_jan %>%
  anti_join(bad_data_set_2)

# now get it

Vehicle_Message_History_90_since_jan_clean %>%
  group_by(Transit_Day) %>%
  summarise(Daily_Boardings = sum(Boards)) %>%
  arrange(desc(Daily_Boardings)) %>%
  View() 
  
#### 6/26/20 update ####

# 6/26/20 - Thomas Coon wants to know average daily ridership and ADR by hour for each of
#           the following stops: 52263; 52264; 52265; 52266; 52302; 52303; 52304

# db connection

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                       Database = "DW_IndyGo", Port = 1433)

# get applicable dates

DimDate_TC <- tbl(con2, "DimDate") %>%
  filter(CalendarDateChar == "10/01/2019") %>%
  collect()

# get all dates

DimDate <- tbl(con2, "DimDate") %>% collect()

# get timekey

DimTime <- tbl(con2, "DimTime") %>% collect()

# get farekey

DimFare <- tbl(con2, "DimFare") %>% collect()

# get stops

DimStop_TC <- tbl(con2, "DimStop") %>%
  filter(StopID %in% c(52263, 52264, 52265, 52266, 52302, 52303, 52304)) %>%
  collect()

library(leaflet)

DimStop_TC %>%
leaflet() %>%
addCircles() %>%
addTiles()

# get FF boarding and alighting

FactFare_TC <- tbl(con2, "FactFare") %>%
  filter(FareKey %in% c(1001, 1002),
         DateKey >= !!DimDate_TC $DateKey,
         StopKey %in% !!DimStop_TC$StopKey) %>%
  collect()

### data check ###

sort(unique(FactFare_TC$DateKey))

### transformatins ###

# get daily averages

FactFare_TC %>%
  left_join(DimDate, by = "DateKey") %>%
  left_join(DimStop_TC, by = "StopKey") %>%
  left_join(DimFare, by = "FareKey") %>%
  group_by(DateKey, StopID, FareReportLabel) %>%
  summarise(Daily_Counts = sum(FareCount)) %>%
  group_by( StopID, FareReportLabel)%>%
  summarise(Average_Daily = round(mean(Daily_Counts), digits = 1)) %>%# pivot this for later
  pivot_wider(names_from = FareReportLabel, values_from = Average_Daily) %>%
  formattable::formattable()

# get averages by work day

FactFare_TC %>%
  left_join(DimDate, by = "DateKey") %>%
  left_join(DimStop_TC, by = "StopKey") %>%
  left_join(DimFare, by = "FareKey") %>%
  group_by(DateKey, StopID, FareReportLabel, WorkdayType) %>%
  summarise(Daily_Counts = sum(FareCount)) %>%
  group_by(WorkdayType, StopID, FareReportLabel)%>%
  summarise(Average_Daily = round(mean(Daily_Counts), digits = 1)) %>%# pivot this for later
  pivot_wider(names_from = c(WorkdayType, FareReportLabel), values_from = Average_Daily) %>%
  formattable::formattable()

# get total by time of day

FactFare_TC %>%
  left_join(DimDate, by = "DateKey") %>%
  left_join(DimStop_TC, by = "StopKey") %>%
  left_join(DimFare, by = "FareKey") %>%
  left_join(DimTime, by = "TimeKey") %>%
  group_by(StopID, FareReportLabel, Time24Hour) %>%
  summarise(Count = sum(FareCount)) %>%
  str()
  
BA_by_hour <- FactFare_TC %>%
  left_join(DimDate, by = "DateKey") %>%
  left_join(DimStop_TC, by = "StopKey") %>%
  left_join(DimFare, by = "FareKey") %>%
  left_join(DimTime, by = "TimeKey") %>%
  group_by(StopID, FareReportLabel, Time24Hour) %>%
  summarise(Count = sum(FareCount)) %>%
  pivot_wider(names_from = c(Time24Hour), values_from = Count ) 

colnames(BA_by_hour ) <- paste(colnames(BA_by_hour), ":00", sep = "")

formattable::formattable(BA_by_hour) # just fix this manually
  
FactFare_TC %>%
    left_join(DimDate, by = "DateKey") %>%
    left_join(DimStop_TC, by = "StopKey") %>%
    left_join(DimFare, by = "FareKey") %>%
    left_join(DimTime, by = "TimeKey") %>%
    group_by(StopID, FareReportLabel, Time24Hour) %>%
    summarise(Daily_Counts = sum(FareCount)) %>%
    filter(FareReportLabel == "Boarding") %>%
    ggplot()+
    geom_bar(aes(x = factor(Time24Hour), y = Daily_Counts),
             stat = "identity")+
    facet_wrap(~StopID) +
    labs(title = "Total Boardings by Stop and Hour of Day",
         subtitle = "October 1, 2019 - June 23, 2020",
         x = "Hour of Day", y = "Count")
  
  FactFare_TC %>%
    left_join(DimDate, by = "DateKey") %>%
    left_join(DimStop_TC, by = "StopKey") %>%
    left_join(DimFare, by = "FareKey") %>%
    left_join(DimTime, by = "TimeKey") %>%
    group_by(StopID, FareReportLabel, Time24Hour) %>%
    summarise(Daily_Counts = sum(FareCount)) %>%
    filter(FareReportLabel == "Alighting") %>%
    ggplot()+
    geom_bar(aes(x = factor(Time24Hour), y = Daily_Counts),
             stat = "identity")+
    facet_wrap(~StopID) + 
    labs(title = "Total Alightings by Stop and Hour of Day",
         subtitle = "October 1, 2019 - June 23, 2020",
         x = "Hour of Day", y = "Count")
  
  ### 7/6/20 Update ###
  

# 2020/09/09 update -------------------------------------------------------
#briometrix wants data for a few stops
  library(dplyr)
  library(timeDate)
  library(data.table)
  library(lubridate)
  library(stringr)
  library(ggplot2)
  
  
  # we need stop boardings by station, and also need wheelchair users for each of the following:
  # Statehouse, Vermont, 9th, IU Health, 18th, 22nd, Fall Creek, 34th, 38th on route 90
  # 38th and Keystone and 38th and Meadows and 38th and Oxford on any other route
  
  #looks like 38th and keystone are 10084 and 10172 and meadows is 10081, while oxford is 10175
  
  #first we'll get passenger trips from VMH
  con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                        Database = "TransitAuthority_IndyGo_Reporting", 
                        Port = 1433)
  
  VMH_StartTime <- "20200301"
  VMH_EndTime <- "20200901"
  
  
  #grab 90 and 901/902 just in case
  #and also get stops
  VMH_Raw <- tbl(
    con,
    sql(
      paste0(
        "select Vehicle_ID
      ,a.Time
      ,Route
      ,Trip
      ,Route_Name
      ,Boards
      ,Alights
      ,Stop_Id
      ,Latitude
      ,Longitude
      ,GPSStatus
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Route like '90%'
      and a.Time > '",VMH_StartTime,"'
      and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
      and (Boards > 0 OR Alights > 0)
      and Stop_Id <> 0
      
      UNION
      
      select Vehicle_ID
      ,a.Time
      ,Route
      ,Trip
      ,Route_Name
      ,Boards
      ,Alights
      ,Stop_Id
      ,Latitude
      ,Longitude
      ,GPSStatus
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Time > '",VMH_StartTime,"'
      and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
      and Stop_Id in (10084,10172,10081,10175)
      and (Boards > 0 OR Alights > 0)"
      )#end paste
    )#endsql
  ) %>% #end paste0
    collect() %>%
    setDT()
  
  fwrite(VMH_Raw,"data//VMH_Raw.csv")
  
  VMH_Raw <- fread("data//VMH_Raw.csv")
  
  #grab dimstop
  con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", 
                         Port = 1433)
  
  DimStop_raw <- tbl(con2, "DimStop") %>%
    collect() %>%
    setDT()
  
  
  DimStop <- DimStop_raw %>%
    filter(is.na(DeleteDate), !is.na(Latitude), ActiveInd == 1)  
  

  
  #join to VMH
  VMH_joined <- DimStop[VMH_Raw, on = c(StopID = "Stop_Id")]
  
  #do Transit_Day and Service Type
  #get holidays
  #set the holy days
  holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                               "USIndependenceDay", "USLaborDay",
                               "USThanksgivingDay", "USChristmasDay")
  
  holidays_saturday_service <- c("USMLKingsBirthday")
  
  #set sat sun
  holidays_sunday <- holiday(2000:2020, holidays_sunday_service)
  holidays_saturday <- holiday(2000:2020, holidays_saturday_service)
  #set service type column
  
  VMH_joined[
    #prepare date and time
    , c("ClockTime","Date") := list(as.ITime(str_sub(Time, 12, 19)),as.IDate(str_sub(Time, 1, 10)))
    ][
      #label prev day or current
      , DateTest := ifelse(ClockTime<"03:00:00",1,0)
      ][
        , Transit_Day := fifelse(
          DateTest ==1
          ,as_date(Date)-1
          ,as_date(Date)
        )#end fifelse
        ][
          ,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
          ][
            ,Service_Type := case_when(Transit_Day %in% as_date(holidays_saturday@Data) ~ "Saturday"
                                       ,Transit_Day %in% as_date(holidays_sunday@Data) ~ "Sunday"
                                       ,weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday")~"Weekday"
                                       ,TRUE ~ weekdays(Transit_Day)) 
            ]
  
  #clean VMH
  
  VMH_joined_daily <- VMH_joined[
    ,.(
      Boardings = sum(Boards)
      , Alightings = sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ]
  
  VMH_joined_zero <- VMH_joined_daily[Boardings == 0 | Alightings == 0]
  
  #remove zero board or alight veh
  VMH_no_zero <- VMH_joined[!VMH_joined_zero,on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")]
  
  VMH_joined_zero
  
  #graph for obvious outliers
  VMH_joined_daily[!VMH_joined_zero,on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")] %>%
    ggplot(aes(x=Boardings)) +
    geom_histogram(binwidth = 1) +
    stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)
  
  obvious_outlier <- 1250
  
  outlier_apc_vehicles <- VMH_joined_daily[Boardings > obvious_outlier]
  
  #get vehicles < 3 SD from mean
  
  #get 3 sd from mean
  
  #remove outliers, then also vehicles that are three deeves AND have Boardings/alightings diff of greater than 5%
  three_deeves <- VMH_no_zero[
    ,.(
      sum(Boards)
      ,sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ][,sd(V1)*3]+
    VMH_no_zero[
      ,.(
        sum(Boards)
        ,sum(Alights)
      )
      ,.(Transit_Day,Vehicle_ID)
      ][,mean(V1)]
  
  VMH_joined_daily[
    ,`:=`(
      pct_diff = (Boardings - Alightings)/((Boardings + Alightings)/2) 
    )
    ]
  
  #get deeve and big pct
  VMH_three_deeves_big_pct <- VMH_joined_daily[(Boardings > three_deeves & pct_diff > 0.1) |
                                                 (Boardings > three_deeves & pct_diff < -0.1)]
  
  VMH_clean <- VMH_no_zero[!VMH_three_deeves_big_pct, on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")]
  
  
  
  #match all the stops together
  VMH_output <- VMH_clean[
    ,StopDesc_trimmed := fifelse(
      str_detect(StopDesc,"CTC")
      ,str_sub(StopDesc,1,end = str_length(StopDesc)-2)
      ,str_sub(StopDesc,1,end = str_length(StopDesc)-3)
    )
    ][
      ,StopDesc_trimmed := fifelse(
        StopDesc_trimmed == "66th Stat"
        ,"66th Station"
        ,StopDesc_trimmed
      )
      ][
        ,StopDesc_trimmed := fifelse(
          StopDesc_trimmed %ilike% "Meado|Oxf"
          ,"Future Meadows Station"
          ,StopDesc_trimmed
        )
        ][
          ,StopDesc_trimmed := fifelse(
            StopDesc_trimmed %ilike% "Key"
            ,"Future Keystone Station"
            ,StopDesc_trimmed
          )
          ][
            , .(
              Daily_Boardings = sum(Boards)
              ,Daily_Alightings = sum(Alights)
            )
            ,.(StopDesc_trimmed
               ,Service_Type,Transit_Day)
            ][
              #filter for time frame and for service type
              Transit_Day >= ymd(VMH_StartTime) & 
                Transit_Day <= ymd(VMH_EndTime) &
                Service_Type == "Weekday"
              ][
                #get avg boardings
                ,.(
                  Average_Daily_Boardings = round(mean(Daily_Boardings)) 
                  ,Average_Daily_Alightings = round(mean(Daily_Alightings))
                )
                ,.(StopDesc_trimmed,Service_Type)
                ][StopDesc_trimmed %ilike% "Verm|State|9th|IU|18|22|Fall|34|38|Mea|Key"]
  
  
  
  #now get wheelchair counts
  start <- ymd("20200601")
  end <- ymd(VMH_EndTime)
  
  DimRoute_raw <- tbl(con2, "DimRoute") %>% collect()
  
  calendar_dates <- tbl(con2,"DimDate") %>% 
    filter(CalendarDate >= start
           ,CalendarDate <= end) %>%
    collect() %>%
    setDT() %>%
    setkey("DateKey")
  
  FactFare_raw <- tbl(con2, "FactFare") %>%
    filter(DateKey %in% !!calendar_dates$DateKey,FareKey == 1005) %>%
    collect() %>%
    setDT()
  
  FactFare_raw <- fread("data//wheelchair.csv")
  
  FactFare_dates <- calendar_dates[FactFare_raw, on = c(DateKey = "DateKey")]
  
  #do transit day
  FactFare_raw[
    #prepare date and time
    , `:=` (
      #Hour = str_sub(ServiceDateTime,12,19)
      Hour = format(ServiceDateTime, "%H")
      ,Date = str_sub(ServiceDateTime,1,10)
    )
    ][
      #label prev day or current
      , DateTest := ifelse(Hour<"3",1,0)
      ][
        , Transit_Day := fifelse(
          DateTest ==1
          ,as_date(Date)-1
          ,as_date(Date)
        )#end fifelse
        ][
          ,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
          ]
  
  FactFare_raw[
    ,Service_Type := fcase(
      Transit_Day %in% as_date(holidays_saturday@Data), "Saturday"
      ,Transit_Day %in% as_date(holidays_sunday@Data), "Sunday"
      ,weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday"), "Weekday"
      ,weekdays(Transit_Day) == "Saturday", "Saturday"
      ,weekdays(Transit_Day) == "Sunday", "Sunday"
    )#end fcase 
    ]
  
  FactFare_raw_no_zero_joined <- DimStop[FactFare_raw[FareCount > 0], on = c(StopKey = "StopKey")]
  
  
  FactFare_output <- FactFare_raw_no_zero_joined[
    ,StopDesc_trimmed := fifelse(
      str_detect(StopDesc,"CTC")
      ,str_sub(StopDesc,1,end = str_length(StopDesc)-2)
      ,str_sub(StopDesc,1,end = str_length(StopDesc)-3)
    )
    ][
      ,StopDesc_trimmed := fifelse(
        StopDesc_trimmed == "66th Stat"
        ,"66th Station"
        ,StopDesc_trimmed
      )
      ][
        ,StopDesc_trimmed := fifelse(
          StopDesc_trimmed %ilike% "Meado|Oxf"
          ,"Future Meadows Station"
          ,StopDesc_trimmed
        )
        ][
          ,StopDesc_trimmed := fifelse(
            StopDesc_trimmed %ilike% "Key"
            ,"Future Keystone Station"
            ,StopDesc_trimmed
          )
          ][
            , .(
              Daily_Wheelchair_Users = sum(FareCount)
            )
            ,.(StopDesc_trimmed
               ,Service_Type,Transit_Day)
            ][
              #filter for time frame and for service type
              Transit_Day >= ymd(VMH_StartTime) & 
                Transit_Day <= ymd(VMH_EndTime) &
                Service_Type == "Weekday"
              ][
                #get avg boardings
                ,.(
                  Average_Daily_Wheelchair_Users = round(mean(Daily_Wheelchair_Users))
                )
                ,.(StopDesc_trimmed,Service_Type)
                ][StopDesc_trimmed %ilike% "(Verm|State|9th|IU|18|22|Fall|34|38|Mea|Key).*ion$"]
  
  
  joined_output <- FactFare_output[
    VMH_output,
    on = c(StopDesc_trimmed = "StopDesc_trimmed",
           Service_Type = "Service_Type")
    ][
      ,.(
        Stop_Name = StopDesc_trimmed
        ,Average_Daily_Boardings
        ,Average_Daily_Alightings
        ,Average_Daily_Wheelchair_Users
      )
      ]
  
  fwrite(joined_output,"data\\BrioMetrix_output.csv")
  


# 2020/09/10 update -------------------------------------------------------

#annette wants to know boardings and alightings for 66th sb
  library(dplyr)
  library(timeDate)
  library(data.table)
  library(lubridate)
  library(stringr)
  library(ggplot2)
  #first we'll get passenger trips from VMH
  con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                        Database = "TransitAuthority_IndyGo_Reporting", 
                        Port = 1433)
  
  VMH_StartTime <- "20190901"
  VMH_EndTime <- "20200901"
  
  con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", 
                         Port = 1433)
  
  DimStop_raw <- tbl(con2, "DimStop") %>%
    collect() %>%
    setDT()
  
  
  DimStop <- DimStop_raw %>%
    filter(is.na(DeleteDate), !is.na(Latitude), ActiveInd == 1)  
  
  VMH_Raw[,sum(Boards),Transit_Day] %>%
    ggplot(aes(x = V1)) +
    geom_histogram()
  
  DimStop_college <- DimStop_raw[StopID == 52093 & ActiveInd == 1 & StopActive == 1]
  
  #grab 90 and 901/902 just in case
  #and also get stops
  VMH_Raw <- tbl(
    con,
    sql(
      paste0(
        "select Vehicle_ID
      ,a.Time
      ,Route
      ,Trip
      ,Route_Name
      ,Boards
      ,Alights
      ,Stop_Id
      ,Latitude
      ,Longitude
      ,GPSStatus
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Route like '90%'
      and a.Time > '",VMH_StartTime,"'
      and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
      and (Boards > 0 OR Alights > 0)
      and Stop_Id = 52093
      "
      )#end paste
    )#endsql
  ) %>% #end paste0
    collect() %>%
    setDT()
  
  #do Transit_Day and Service Type
  #get holidays
  #set the holy days
  holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                               "USIndependenceDay", "USLaborDay",
                               "USThanksgivingDay", "USChristmasDay")
  
  holidays_saturday_service <- c("USMLKingsBirthday")
  
  #set sat sun
  holidays_sunday <- holiday(2000:2020, holidays_sunday_service)
  holidays_saturday <- holiday(2000:2020, holidays_saturday_service)
  #set service type column
  
  VMH_Raw[
    #prepare date and time
    , c("ClockTime","Date") := list(as.ITime(str_sub(Time, 12, 19)),as.IDate(str_sub(Time, 1, 10)))
    ][
      #label prev day or current
      , DateTest := ifelse(ClockTime<"03:00:00",1,0)
      ][
        , Transit_Day := fifelse(
          DateTest ==1
          ,as_date(Date)-1
          ,as_date(Date)
        )#end fifelse
        ][
          ,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
          ][
            ,Service_Type := case_when(Transit_Day %in% as_date(holidays_saturday@Data) ~ "Saturday"
                                       ,Transit_Day %in% as_date(holidays_sunday@Data) ~ "Sunday"
                                       ,weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday")~"Weekday"
                                       ,TRUE ~ weekdays(Transit_Day)) 
            ]
  
  VMH_joined_daily <- VMH_Raw[
    ,.(
      Boards = sum(Boards)
      , Alights = sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ]
  
  
  #graph for obvious outliers
  VMH_joined_daily[] %>%
    ggplot(aes(x=Boards)) +
    geom_histogram(binwidth = 1) +
    stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)
  
  obvious_outlier <- 200
  
  outlier_apc_vehicles <- VMH_joined_daily[Boardings > obvious_outlier]
  
  #get vehicles < 3 SD from mean
  
  #get 3 sd from mean
  
  #remove outliers, then also vehicles that are three deeves AND have Boardings/alightings diff of greater than 5%
  three_deeves <- VMH_joined_daily[
    ,.(
      sum(Boards)
      ,sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ][,sd(V1)*3]+
    VMH_joined_daily[
      ,.(
        sum(Boards)
        ,sum(Alights)
      )
      ,.(Transit_Day,Vehicle_ID)
      ][,mean(V1)]
  
  three_deeves
  
  VMH_joined_daily[
    ,`:=`(
      pct_diff = (Boards - Alights)/((Boards + Alights)/2) 
    )
    ]
  
  #get deeve and big pct
  VMH_three_deeves_big_pct <- VMH_joined_daily[(Boards > three_deeves & pct_diff > 0.1) |
                                                 (Boards > three_deeves & pct_diff < -0.1)]
  
  VMH_clean <- VMH_Raw[!VMH_three_deeves_big_pct, on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")]

  pre_cov_avg <- VMH_clean[Transit_Day <= "2020-03-14",sum(Boards),Transit_Day][,mean(V1)]
  
  pre_cov_avg
  
  post_cov_avg <- VMH_clean[Transit_Day > "2020-03-14",sum(Boards),Transit_Day][,mean(V1)]

  post_cov_avg
  
  pre_cov_alight <- VMH_clean[Transit_Day <= "2020-03-14",sum(Alights),Transit_Day][,mean(V1)]
  
  pre_cov_alight

  post_cov_alight <- VMH_clean[Transit_Day > "2020-03-14",sum(Alights),Transit_Day][,mean(V1)]
  
  post_cov_alight
  
      
# 2020/09/15 update ---------------------------------------------------------
#aaron wants to know highest redline riderhsip day before march after jan 20ish
  library(dplyr)
  library(timeDate)
  library(data.table)
  library(lubridate)
  library(stringr)
  library(ggplot2)
  #first we'll get passenger trips from VMH
  con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                        Database = "TransitAuthority_IndyGo_Reporting", 
                        Port = 1433)
  
  VMH_StartTime <- "20200119"
  VMH_EndTime <- "20200301"
  
  
  #grab 90 and 901/902 just in case
  #and also get stops
  VMH_Raw <- tbl(
    con,
    sql(
      paste0(
        "select Vehicle_ID
      ,a.Time
      ,Route
      ,Trip
      ,Route_Name
      ,Boards
      ,Alights
      ,Stop_Id
      ,Latitude
      ,Longitude
      ,GPSStatus
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Route like '90%'
      and a.Time > '",VMH_StartTime,"'
      and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
      and (Boards > 0 OR Alights > 0)
      and Stop_Id <> 0
      "
      )#end paste
    )#endsql
  ) %>% #end paste0
    collect() %>%
    setDT()
  
  #fwrite(VMH_Raw,"data//VMH_Raw.csv")
  
  #VMH_Raw <- fread("data//VMH_Raw.csv")
  
  #do Transit_Day and Service Type
  #get holidays
  #set the holy days
  holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                               "USIndependenceDay", "USLaborDay",
                               "USThanksgivingDay", "USChristmasDay")
  
  holidays_saturday_service <- c("USMLKingsBirthday")
  
  #set sat sun
  holidays_sunday <- holiday(2000:2020, holidays_sunday_service)
  holidays_saturday <- holiday(2000:2020, holidays_saturday_service)
  #set service type column
  
  VMH_Raw[
    #prepare date and time
    , c("ClockTime","Date") := list(as.ITime(str_sub(Time, 12, 19)),as.IDate(str_sub(Time, 1, 10)))
    ][
      #label prev day or current
      , DateTest := ifelse(ClockTime<"03:00:00",1,0)
      ][
        , Transit_Day := fifelse(
          DateTest ==1
          ,as_date(Date)-1
          ,as_date(Date)
        )#end fifelse
        ][
          ,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
          ][
            ,Service_Type := case_when(Transit_Day %in% as_date(holidays_saturday@Data) ~ "Saturday"
                                       ,Transit_Day %in% as_date(holidays_sunday@Data) ~ "Sunday"
                                       ,weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday")~"Weekday"
                                       ,TRUE ~ weekdays(Transit_Day)) 
            ]
  
  #clean VMH
  
  VMH_joined_daily <- VMH_Raw[
    ,.(
      Boardings = sum(Boards)
      , Alightings = sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ]
  
  VMH_joined_zero <- VMH_joined_daily[Boardings == 0 | Alightings == 0]
  
  #remove zero board or alight veh
  VMH_no_zero <- VMH_Raw[!VMH_joined_zero,on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")]
  
  VMH_joined_zero
  
  #graph for obvious outliers
  VMH_joined_daily[!VMH_joined_zero,on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")] %>%
    ggplot(aes(x=Boardings)) +
    geom_histogram(binwidth = 1) +
    stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)
  
  obvious_outlier <- 1250
  
  outlier_apc_vehicles <- VMH_joined_daily[Boardings > obvious_outlier]
  
  #get vehicles < 3 SD from mean
  
  #get 3 sd from mean
  
  #remove outliers, then also vehicles that are three deeves AND have Boardings/alightings diff of greater than 5%
  three_deeves <- VMH_no_zero[
    ,.(
      sum(Boards)
      ,sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ][,sd(V1)*3]+
    VMH_no_zero[
      ,.(
        sum(Boards)
        ,sum(Alights)
      )
      ,.(Transit_Day,Vehicle_ID)
      ][,mean(V1)]
  
  VMH_joined_daily[
    ,`:=`(
      pct_diff = (Boardings - Alightings)/((Boardings + Alightings)/2) 
    )
    ]
  
  #get deeve and big pct
  VMH_three_deeves_big_pct <- VMH_joined_daily[(Boardings > three_deeves & pct_diff > 0.1) |
                                                 (Boardings > three_deeves & pct_diff < -0.1)]
  
  VMH_clean <- VMH_no_zero[!VMH_three_deeves_big_pct, on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")]
  
  
  
  VMH_clean[,.(Boardings = sum(Boards)),.(Transit_Day,Vehicle_ID)] %>%
    ggplot(aes(x=Boardings)) +
    geom_histogram(binwidth = 1) +
    stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)
  
  
  VMH_clean[Route == 90,.(Boardings = sum(Boards)),Transit_Day][order(Boardings,decreasing = T)]


# 2020/10/01 Roth  --------------------------------------------------------
  library(dplyr)
  library(timeDate)
  library(data.table)
  library(lubridate)
  library(stringr)
  library(ggplot2)
  
  con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", 
                           Server = "AVAILDWHP01VW", Database = "DW_IndyGo", Port = 1433)
  
  #now get wheelchair counts
  start <- ymd("20200601")
  end <- ymd(Sys.Date())
  
  DimRoute <- tbl(con_DW, "DimRoute") %>% 
    collect() %>% 
    data.table(key = "RouteKey")
  
  DimStop <- tbl(con_DW,"DimStop") %>% 
    collect() %>% 
    data.table(key = "StopKey")
  
  calendar_dates <- tbl(con_DW,"DimDate") %>% 
    filter(CalendarDate >= start
           ,CalendarDate <= end) %>%
    collect() %>%
    data.table(key = "DateKey")
  
  FactFare_raw <- tbl(con_DW, "FactFare") %>%
    filter(DateKey %in% !!calendar_dates$DateKey,FareKey == 1005) %>%
    collect() %>%
    data.table()
  
  FactFare_dates_no_zero <- calendar_dates[FactFare_raw[FareCount > 0], on = c(DateKey = "DateKey")]
  
    #do transit day and service type
  #get holidays
  #set the holy days
  holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                               "USIndependenceDay", "USLaborDay",
                               "USThanksgivingDay", "USChristmasDay")
  
  holidays_saturday_service <- c("USMLKingsBirthday")
  
  #set sat sun
  holidays_sunday <- holiday(2000:2020, holidays_sunday_service)
  holidays_saturday <- holiday(2000:2020, holidays_saturday_service)
  
  
  FactFare_dates_no_zero[
    #prepare date and time
    , `:=` (
      #Hour = str_sub(ServiceDateTime,12,19)
      Hour = format(ServiceDateTime, "%H")
      ,Date = str_sub(ServiceDateTime,1,10)
    )
    ][
      #label prev day or current
      , DateTest := ifelse(Hour<"3",1,0)
      ][
        , Transit_Day := fifelse(
          DateTest ==1
          ,as_date(Date)-1
          ,as_date(Date)
        )#end fifelse
        ][
          ,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
          ]
  
  FactFare_dates_no_zero[
    ,Service_Type := fcase(
      Transit_Day %in% as_date(holidays_saturday@Data), "Saturday"
      ,Transit_Day %in% as_date(holidays_sunday@Data), "Sunday"
      ,weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday"), "Weekday"
      ,weekdays(Transit_Day) == "Saturday", "Saturday"
      ,weekdays(Transit_Day) == "Sunday", "Sunday"
    )#end fcase 
    ]
  
  FactFare_dates_no_zero_joined <- DimStop[FactFare_dates_no_zero, on = c(StopKey = "StopKey")][
    DimRoute
    ,on = "RouteKey"
    ,names(DimRoute) := mget(paste0("i.",names(DimRoute)))
  ]
  
  FactFare_output <- 1
  
  
  FactFare_dates_no_zero_joined[
    Service_Type == "Weekday" & RouteReportLabel != 90
  ][
    , .(
      Total_Wheelchair_Users = sum(FareCount)
      )
    ,.(Route = RouteReportLabel,Month = month(Transit_Day))
  ][
    order(Month,Route)
  ][
    Route != "Deadhead"
  ][
    ,Month := month.name[Month]
  ] %>%
    gt::gt()
  
  FactFare_dates_no_zero_joined[
    Service_Type == "Weekday" & RouteReportLabel != 90
    ][
      , .(
        Total_Wheelchair_Users = sum(FareCount)
      )
      ,.(Route = RouteReportLabel,Transit_Day)
      ][
        order(Transit_Day,Route)
        ][
          Route != "Deadhead"
          ] %>%
    gt::gt()
  

# 2020/10/06 Aaron Vogel --------------------------------------------------
  library(dplyr)
  library(timeDate)
  library(data.table)
  library(lubridate)
  library(stringr)
  library(ggplot2)
  library(leaflet)
  #first we'll get passenger trips from VMH
  con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                        Database = "TransitAuthority_IndyGo_Reporting", 
                        Port = 1433)
  
  VMH_StartTime <- "20200601"
  VMH_EndTime <- "20200630"
  
  
  #grab 90 and 901/902 just in case
  #and also get stops
  VMH_Raw <- tbl(
    con,
    sql(
      paste0(
        "select Vehicle_ID
      ,a.Time
      ,Route
      ,Trip
      ,Route_Name
      ,Boards
      ,Alights
      ,Stop_Id
      ,Latitude
      ,Longitude
      ,GPSStatus
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Route like '90%'
      and a.Time > '",VMH_StartTime,"'
      and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
      and (Boards > 0 OR Alights > 0)
      and Stop_Id <> 0
      "
      )#end paste
    )#endsql
  ) %>% #end paste0
    collect() %>%
    setDT()
  
  #fwrite(VMH_Raw,"data//VMH_Raw.csv")
  
  #VMH_Raw <- fread("data//VMH_Raw.csv")
  
  #do Transit_Day and Service Type
  #get holidays
  #set the holy days
  holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                               "USIndependenceDay", "USLaborDay",
                               "USThanksgivingDay", "USChristmasDay")
  
  holidays_saturday_service <- c("USMLKingsBirthday")
  
  #set sat sun
  holidays_sunday <- holiday(2000:2020, holidays_sunday_service)
  holidays_saturday <- holiday(2000:2020, holidays_saturday_service)
  #set service type column
  
  VMH_Raw[
    #prepare date and time
    , c("ClockTime","Date") := list(as.ITime(str_sub(Time, 12, 19)),as.IDate(str_sub(Time, 1, 10)))
    ][
      #label prev day or current
      , DateTest := ifelse(ClockTime<"03:00:00",1,0)
      ][
        , Transit_Day := fifelse(
          DateTest ==1
          ,as_date(Date)-1
          ,as_date(Date)
        )#end fifelse
        ][
          ,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
          ][
            ,Service_Type := case_when(Transit_Day %in% as_date(holidays_saturday@Data) ~ "Saturday"
                                       ,Transit_Day %in% as_date(holidays_sunday@Data) ~ "Sunday"
                                       ,weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday")~"Weekday"
                                       ,TRUE ~ weekdays(Transit_Day)) 
            ]
  
  
  #clean VMH
  
  VMH_joined_daily <- VMH_Raw[
    ,.(
      Boardings = sum(Boards)
      , Alightings = sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ]
  
  VMH_joined_zero <- VMH_joined_daily[Boardings == 0 | Alightings == 0]
  
  #remove zero board or alight veh
  VMH_no_zero <- VMH_Raw[!VMH_joined_zero,on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")]
  
  
  #graph for obvious outliers
  VMH_joined_daily[!VMH_joined_zero,on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")] %>%
    ggplot(aes(x=Boardings)) +
    geom_histogram(binwidth = 1) +
    stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)
  
  obvious_outlier <- 260
  
  outlier_apc_vehicles <- VMH_joined_daily[Boardings > obvious_outlier]
  
  
  #get vehicles < 3 SD from mean
  #get 3 sd from mean
  #remove outliers, then also vehicles that are three deeves AND have Boardings/alightings diff of greater than 5%
  three_deeves <- VMH_no_zero[!outlier_apc_vehicles,on = c("Transit_Day","Vehicle_ID")
  ][
    ,.(
      sum(Boards)
      ,sum(Alights)
      )
    ,.(Transit_Day,Vehicle_ID)
    ][
      ,sd(V1)*3]+
    VMH_no_zero[!outlier_apc_vehicles,on = c("Transit_Day","Vehicle_ID")
][,.(
        sum(Boards)
        ,sum(Alights)
        )
      ,.(Transit_Day,Vehicle_ID)
      ][,mean(V1)]
  
  VMH_joined_daily[
    ,`:=`(
      pct_diff = (Boardings - Alightings)/((Boardings + Alightings)/2) 
    )
    ]
  
  #get deeve and big pct
  VMH_three_deeves_big_pct <- VMH_joined_daily[(Boardings > three_deeves & pct_diff > 0.1) |
                                                 (Boardings > three_deeves & pct_diff < -0.1)]
  
  
  
  VMH_clean <- VMH_no_zero[
    
    #remove three deeves
    !VMH_three_deeves_big_pct, on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")
  ][
    #remove outliers
    !outlier_apc_vehicles, on = c("Transit_Day","Vehicle_ID")
  ][
    #remove 1899 on 2020-06-30
    Vehicle_ID != 1899
  ][
    #remove this vehicle this day as well
    !VMH_joined_daily[Vehicle_ID==1992 & Transit_Day == "2020-06-06"]
    , on = c("Transit_Day","Vehicle_ID")
  ]
  
  
  
  VMH_Aaron_Summary <-VMH_clean[
    ,.(Boardings = sum(Boards),
       Alightings = sum(Alights))
    ,.(Transit_Day,Vehicle_ID) 
  ]
  
  fwrite(VMH_Aaron_Summary,"data//VMH_Aaron_Summary.csv")
  
  #the following are "bad"
  # Transit_Day Vehicle_ID Boardings
  # 1:  2020-06-04       1899       403
  # 2:  2020-06-26       1981       208
  # 3:  2020-06-28       1992       266
  # 4:  2020-06-29       1992       215
  # 5:  2020-06-29       1978       246
  # 6:  2020-06-29       1986       315
  # 7:  2020-06-30       1977       242
  # 8:  2020-06-30       1899       203
  


# 2020/10/19 Aletra -------------------------------------------------------
library(dplyr)
library(timeDate)
library(data.table)
library(lubridate)
library(stringr)
library(ggplot2)
library(leaflet)
#first we'll get passenger trips from VMH
con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

VMH_StartTime <- "20200201"
VMH_EndTime <- "20200930"

#grab 90 and 901/902 just in case
#and also get stops
VMH_Raw <- tbl(
  con,
  sql(
    paste0(
      "select Vehicle_ID
      ,a.Time
      ,Route
      ,Trip
      ,Route_Name
      ,Boards
      ,Alights
      ,Stop_Id
      ,Latitude
      ,Longitude
      ,GPSStatus
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Route like '90%'
      and a.Time > '",VMH_StartTime,"'
      and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
      and (Boards > 0 OR Alights > 0)
      and Stop_Id <> 0
      "
    )#end paste
  )#endsql
) %>% #end paste0
  collect() %>%
  setDT()

fwrite(VMH_Raw,"data//VMH_Raw.csv")

VMH_Raw <- fread("data//VMH_Raw.csv")


#do Transit_Day and Service Type
#get holidays
#set the holy days
holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

#set sat sun
holidays_sunday <- holiday(2000:2020, holidays_sunday_service)
holidays_saturday <- holiday(2000:2020, holidays_saturday_service)
#set service type column

VMH_Raw[
  #prepare date and time
  , c("ClockTime","Date") := list(as.ITime(str_sub(Time, 12, 19)),as.IDate(str_sub(Time, 1, 10)))
  ][
    #label prev day or current
    , DateTest := ifelse(ClockTime<"03:00:00",1,0)
    ][
      , Transit_Day := fifelse(
        DateTest ==1
        ,as_date(Date)-1
        ,as_date(Date)
      )#end fifelse
      ][
        ,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
        ][
          ,Service_Type := case_when(Transit_Day %in% as_date(holidays_saturday@Data) ~ "Saturday"
                                     ,Transit_Day %in% as_date(holidays_sunday@Data) ~ "Sunday"
                                     ,weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday")~"Weekday"
                                     ,TRUE ~ weekdays(Transit_Day)) 
          ]


#clean VMH

VMH_joined_daily <- VMH_Raw[
  ,.(
    Boardings = sum(Boards)
    , Alightings = sum(Alights)
  )
  ,.(Transit_Day,Vehicle_ID)
  ]



VMH_joined_zero <- VMH_joined_daily[Boardings == 0 | Alightings == 0]

#remove zero board or alight veh
VMH_no_zero <- VMH_Raw[!VMH_joined_zero,on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")]


#graph for obvious outliers
VMH_joined_daily[!VMH_joined_zero,on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")][Boardings > 300] %>%
  ggplot(aes(x=Boardings)) +
  geom_histogram(binwidth = 1) +
  stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)

obvious_outlier <- 500

outlier_apc_vehicles <- VMH_joined_daily[Boardings > obvious_outlier]


#get vehicles < 3 SD from mean
#get 3 sd from mean
#remove outliers, then also vehicles that are three deeves AND have Boardings/alightings diff of greater than 5%
three_deeves <- VMH_no_zero[!outlier_apc_vehicles,on = c("Transit_Day","Vehicle_ID")
                            ][
                              ,.(
                                sum(Boards)
                                ,sum(Alights)
                              )
                              ,.(Transit_Day,Vehicle_ID)
                              ][
                                ,sd(V1)*3]+
  VMH_no_zero[!outlier_apc_vehicles,on = c("Transit_Day","Vehicle_ID")
              ][,.(
                sum(Boards)
                ,sum(Alights)
              )
              ,.(Transit_Day,Vehicle_ID)
              ][,mean(V1)]

VMH_joined_daily[
  ,`:=`(
    pct_diff = (Boardings - Alightings)/((Boardings + Alightings)/2) 
  )
  ]

#get deeve and big pct
VMH_three_deeves_big_pct <- VMH_joined_daily[(Boardings > three_deeves & pct_diff > 0.1) |
                                               (Boardings > three_deeves & pct_diff < -0.1)]



VMH_clean <- VMH_no_zero[
  
  #remove three deeves
  !VMH_three_deeves_big_pct, on = c(Transit_Day = "Transit_Day",Vehicle_ID = "Vehicle_ID")
][
  #remove outliers
  !outlier_apc_vehicles, on = c("Transit_Day","Vehicle_ID")
][
  #remove 1899 on 2020-06-30
  Vehicle_ID != 1899
][
  #remove this vehicle this day as well
  !VMH_joined_daily[Vehicle_ID==1992 & Transit_Day == "2020-06-06"]
  , on = c("Transit_Day","Vehicle_ID")
][
  #get red line set
  Latitude > 39.709468 & Latitude < 39.877512
][
  #take out garage
  Longitude > -86.173321
]


summary <- VMH_clean[
  #get hourly boardings by day
  ,.(
    Boards = sum(Boards)
    ,Alight = sum(Alights)
  )
  #group by hour, day, service_type
  ,.(
    hour(Time)
    ,Transit_Day
    ,Service_Type
  )
  ][
    order(month(Transit_Day),hour)      
    ,.(Average_Boardings = round(mean(Boards)))
    ,.(
      hour = hour
      ,month = month(Transit_Day)
      ,Service_Type
    )
    ]

Aletra_Hourly_Summary <- dcast(summary,month + hour ~ Service_Type,fill = 0)[
  ,month := month.abb[month]
][
  ,hour := paste0(hour,":00")
][]




fwrite(Aletra_Hourly_Summary,"data//Aletra_Hourly_Summary.csv")  


# 2020/10/29

# 2020/10/29 Service Planning Station Activity ----------------------------
#two date ranges, Jan - MArch 14th
#june - Oct 1
library(dplyr)
library(data.table)
library(ggplot2)


# Import ------------------------------------------------------------------



# db connection

con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", Port = 1433)


# get applicable dates

DimServiceLevel <- tbl(con_DW,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")

DimDate_start_pre_cov <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "01/01/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_end_pre_cov <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "03/14/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")


DimDate_full <- tbl(con_DW, "DimDate") %>%
  collect() %>% 
  setDT(key = "DateKey")

# get stops
DimStop <- tbl(con_DW,"DimStop") %>% 
  collect() %>% 
  setDT(key = "StopKey")

# get farekey

DimFare <- tbl(con_DW, "DimFare") %>%
  collect() %>%
  setDT(key = "FareKey")

# get Routes
DimRoute <- tbl(con_DW,"DimRoute") %>%
  filter() %>%
  collect() %>% 
  setDT(key = "RouteKey")

# get FF boarding and alighting

FactFare <- tbl(con_DW, "FactFare") %>%
  filter(
    FareKey %in% c(1001, 1002),
    DateKey >= local(DimDate_start_pre_cov$DateKey),
    DateKey <= local(DimDate_end_pre_cov$DateKey)
  ) %>% #end filter
  collect() %>% 
  setDT()

#add Stop Ridership Col Names
Stop_Ridership_col_names <- c("Stop Number", "Stop Name", "Passengers On", "Average Daily On","Passengers Off","Average Daily Off","Days")


### transformations ###

# Get Weekday only --------------------------------------------------------


#join servicelevel and a bit of stop info and dimfare while we're here

#mget for memory efficiency, keeps location the same
FactFare[
  #skinny stops first
  DimStop[,.(StopKey,StopID)]
  ,on = "StopKey"
  ,names(DimStop[,.(StopKey,StopID)]) := mget(paste0("i.",names(DimStop[,.(StopKey,StopID)])))
  ][
    #then service level skinny
    DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]
    ,on = "ServiceLevelKey"
    ,names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]) := mget(paste0("i.",names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)])))
    ][
      #then dimfareskinny
      DimFare[,.(FareKey,FareReportLabel)]
      , on = "FareKey"
      ,names(DimFare[,.(FareKey,FareReportLabel)]) := mget(paste0("i.",names(DimFare[,.(FareKey,FareReportLabel)])))
      ][,Service_Type := sub(".*-","",ServiceLevelReportLabel)
        ]



#check service
FactFare[,uniqueN(DateKey),.(sub(".*-","",ServiceLevelReportLabel))]

FactFare[,uniqueN(DateKey),Service_Type]


# count weekdays appearing for each stop ----------------------------------
FactFare_joined_weekday <- FactFare[Service_Type == "Weekday"]

#first we need the number of days each stop appears on weekdays only
FactFareDateCount <- 
  #filter for weekdays
  FactFare[Service_Type == "Weekday"][
    #get n date by stopid
    ,.(Days = uniqueN(DateKey))
    ,StopID
    ]

# FactFare_joined <- DimServiceLevel[
#   DimRoute[
#     DimStop[,.(StopKey,StopID)][
#       DimDate_full[
#         DimFare[
#           FactFare[FareCount > 0]
#           ,on = "FareKey"
#           ]
#         ,on = "DateKey"
#         ]
#       ,on = "StopKey"
#       ]
#     ,on = "RouteKey"
#     ]
#   ,on = "ServiceLevelKey"
# ]
# 
# FactFare_joined[,Service_Type := sub(".*-","",ServiceLevelReportLabel)]
# 
# #check service
# FactFare_joined[,uniqueN(DateKey),.(sub(".*-","",ServiceLevelReportLabel))]

# FactFare_joined[,uniqueN(DateKey),Service_Type]
# 
# FactFare_joined_weekday <- FactFare_joined[Service_Type == "Weekday"]


# FactFare[
#   DimRoute
#   ,on = "RouteKey"
#   ,names(DimRoute) := mget(paste0("i.",names(DimRoute)))
# ][
#   DimStop
#   ,on = "StopKey"
#   ,names(DimStop) := mget(paste0("i.",names(DimStop)))
# ][
#   DimDate_full
#   ,on = "DateKey"
#   ,names(DimDate_full) := mget(paste0("i.",names(DimDate_full)))
# ][DimFare
#   ,on = "FareKey"
#   ,names(DimFare) := mget(paste0("i.",names(DimDate_full)))
# ]



# FF_joined <- DimStop[
#   DimDate_full[
#     
#       DimFare[
#         DimRoute[
#           FactFare
#           , on = "RouteKey"
#           ]
#         , on = "FareKey"
#         ]
#     , on = "DateKey"
#     ]
#   , on = "StopKey"
# ]


# get daily vehicle boards, then clean bad vehicles -----------------------


#get daily by vehicle
FF_joined_daily <- FactFare[
  ,sum(FareCount)
  ,.(FareReportLabel,DateKey,VehicleKey)
  ][
    ,dcast(
      .SD
      ,DateKey + VehicleKey ~ FareReportLabel
      ,fill = 0
    )
    ]

#get zero days
FF_joined_zero <- FF_joined_daily[Boarding == 0 | Alighting == 0]

#remove them
FF_no_zero <- FactFare_joined_weekday[!FF_joined_zero,on = c(DateKey = "DateKey",VehicleKey = "VehicleKey")]

#graph it and check
FF_joined_daily[!FF_joined_zero,on = c(DateKey = "DateKey",VehicleKey = "VehicleKey")] %>%
  ggplot(aes(x=Boarding)) +
  geom_histogram(binwidth = 1) +
  stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)

#okay, 500 is a good bet i think
obvious_outlier <- 500

outlier_apc_vehicles <- FF_joined_daily[Boarding > obvious_outlier]

#remove outliers, get SD, then remove vehicles > three deeves and have b/a diff > 5%
three_deeves <- FF_no_zero[
  !outlier_apc_vehicles, on = c("DateKey","VehicleKey")
  ][
    ,sum(FareCount)
    ,.(FareReportLabel,DateKey,VehicleKey)
    ][
      ,dcast(
        .SD
        ,DateKey + VehicleKey ~ FareReportLabel
        ,fill = 0
      )
      ][,sd(Boarding)*3]+
  FF_no_zero[
    !outlier_apc_vehicles, on = c("DateKey","VehicleKey")
    ][
      ,sum(FareCount)
      ,.(FareReportLabel,DateKey,VehicleKey)
      ][
        ,dcast(
          .SD
          ,DateKey + VehicleKey ~ FareReportLabel
          ,fill = 0
        )
        ][,mean(Boarding)]

#add pct diff
FF_joined_daily[
  ,pct_diff := (Boarding - Alighting)/((Boarding + Alighting)/2)
  ]

#get deeve and big pct
FF_three_deeves_big_pct <- FF_joined_daily[(Boarding > three_deeves & pct_diff > 0.05) | 
                                             (Boarding > three_deeves & pct_diff < -0.1)]

FF_clean <- FF_no_zero[
  #remove three deeves
  !FF_three_deeves_big_pct, on = c("DateKey","VehicleKey")
  ]




# get boards by stop ------------------------------------------------------


#get daily sums by stopID
DailyStop <- FF_clean[
  #remove garage and 0
  StopID != 0 & StopID < 99000
  ,sum(FareCount)
  ,.(FareReportLabel,DateKey,StopID)
  ]

#get total boards for each stop
StopSums <- dcast(DailyStop[,sum(V1),.(StopID,FareReportLabel)],
                  StopID ~ FareReportLabel)


# get average daily boarding and alighting -------------------------------

#join stop sums with date count, then get rounded average
FF_pc_summary <- DimStop[DimStop[,.I[StopKey == max(StopKey)],by = StopID]$V1][,.(StopID,StopDesc)][
  merge.data.table(StopSums,FactFareDateCount,"StopID",all.x = T) %>%
    .[
      ,`:=`(
        Average_Boardings = round(Boarding/Days,1)
        ,Average_Alightings = round(Alighting/Days,1)
      )#end :=
      ]
  ,on = c(StopID = "StopID")
  ]

setcolorder(FF_pc_summary,c(1,2,4,6,3,7))

setnames(FF_pc_summary,names(FF_pc_summary),Stop_Ridership_col_names)

fwrite(FF_pc_summary,"data//FF_pc_summary.csv")


# okay now we do post cov, should be easy now that we have it -------------

# Import ------------------------------------------------------------------


# db connection

con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", Port = 1433)


# get applicable dates

DimServiceLevel <- tbl(con_DW,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")

DimDate_start_post_cov <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "06/01/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_end_post_cov <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "10/01/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")


DimDate_full <- tbl(con_DW, "DimDate") %>%
  collect() %>% 
  setDT(key = "DateKey")

# get stops
DimStop <- tbl(con_DW,"DimStop") %>% 
  collect() %>% 
  setDT(key = "StopKey")

# get farekey

DimFare <- tbl(con_DW, "DimFare") %>%
  collect() %>%
  setDT(key = "FareKey")

# get Routes
DimRoute <- tbl(con_DW,"DimRoute") %>%
  filter() %>%
  collect() %>% 
  setDT(key = "RouteKey")

# get FF boarding and alighting

FactFare <- tbl(con_DW, "FactFare") %>%
  filter(
    FareKey %in% c(1001, 1002),
    DateKey >= local(DimDate_start_post_cov$DateKey),
    DateKey <= local(DimDate_end_post_cov$DateKey)
  ) %>% #end filter
  collect() %>% 
  setDT()
# Get Weekday only --------------------------------------------------------


#join servicelevel and a bit of stop info and dimfare while we're here

#mget for memory efficiency, keeps location the same
FactFare[
  #skinny stops first
  DimStop[,.(StopKey,StopID)]
  ,on = "StopKey"
  ,names(DimStop[,.(StopKey,StopID)]) := mget(paste0("i.",names(DimStop[,.(StopKey,StopID)])))
  ][
    #then service level skinny
    DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]
    ,on = "ServiceLevelKey"
    ,names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]) := mget(paste0("i.",names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)])))
    ][
      #then dimfareskinny
      DimFare[,.(FareKey,FareReportLabel)]
      , on = "FareKey"
      ,names(DimFare[,.(FareKey,FareReportLabel)]) := mget(paste0("i.",names(DimFare[,.(FareKey,FareReportLabel)])))
      ][,Service_Type := sub(".*-","",ServiceLevelReportLabel)
        ]



#check service
FactFare[,.(uniqueN(DateKey),.N),.(sub(".*-","",ServiceLevelReportLabel))]

FactFare[,.(uniqueN(DateKey),.N),Service_Type]


# count weekdays appearing for each stop ----------------------------------


#first we need the number of days each stop appears on weekdays only
FactFareDateCount <- 
  #filter for weekdays
  FactFare[Service_Type == "Weekday"][
    #get n date by stopid
    ,.(Days = uniqueN(DateKey))
    ,StopID
    ]

FactFare_joined_weekday <- FactFare[Service_Type == "Weekday"]
# get daily vehicle boards, then clean bad vehicles -----------------------


#get daily by vehicle
FF_joined_daily <- FactFare_joined_weekday[
  ,sum(FareCount)
  ,.(FareReportLabel,DateKey,VehicleKey)
  ][
    ,dcast(
      .SD
      ,DateKey + VehicleKey ~ FareReportLabel
      ,fill = 0
    )
    ]

#get zero days
FF_joined_zero <- FF_joined_daily[Boarding == 0 | Alighting == 0]

#remove them
FF_no_zero <- FactFare_joined_weekday[!FF_joined_zero,on = c(DateKey = "DateKey",VehicleKey = "VehicleKey")]

#graph it and check
FF_joined_daily[!FF_joined_zero,on = c(DateKey = "DateKey",VehicleKey = "VehicleKey")] %>%
  ggplot(aes(x=Boarding)) +
  geom_histogram(binwidth = 1) +
  stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)

#okay, 500 is a good bet i think
obvious_outlier <- 500

outlier_apc_vehicles <- FF_joined_daily[Boarding > obvious_outlier]

#remove outliers, get SD, then remove vehicles > three deeves and have b/a diff > 5%
three_deeves <- FF_no_zero[
  !outlier_apc_vehicles, on = c("DateKey","VehicleKey")
  ][
    ,sum(FareCount)
    ,.(FareReportLabel,DateKey,VehicleKey)
    ][
      ,dcast(
        .SD
        ,DateKey + VehicleKey ~ FareReportLabel
        ,fill = 0
      )
      ][,sd(Boarding)*3]+
  FF_no_zero[
    !outlier_apc_vehicles, on = c("DateKey","VehicleKey")
    ][
      ,sum(FareCount)
      ,.(FareReportLabel,DateKey,VehicleKey)
      ][
        ,dcast(
          .SD
          ,DateKey + VehicleKey ~ FareReportLabel
          ,fill = 0
        )
        ][,mean(Boarding)]

#add pct diff
FF_joined_daily[
  ,pct_diff := (Boarding - Alighting)/((Boarding + Alighting)/2)
  ]

#get deeve and big pct
FF_three_deeves_big_pct <- FF_joined_daily[(Boarding > three_deeves & pct_diff > 0.05) | 
                                             (Boarding > three_deeves & pct_diff < -0.1)]

FF_clean <- FF_no_zero[
  #remove three deeves
  !FF_three_deeves_big_pct, on = c("DateKey","VehicleKey")
  ]


# get boards by stop ------------------------------------------------------


#get daily sums by stopID
DailyStop <- FF_clean[
  #remove garage and 0
  StopID != 0 & StopID < 99000
  ,sum(FareCount)
  ,.(FareReportLabel,DateKey,StopID)
  ]

#get total boards for each stop
StopSums <- dcast(DailyStop[,sum(V1),.(StopID,FareReportLabel)],
                  StopID ~ FareReportLabel)


# get average daily boarding and alighting -------------------------------

#join stop sums with date count, then get rounded average
FF_postc_summary <- DimStop[DimStop[,.I[StopKey == max(StopKey)],by = StopID]$V1][,.(StopID,StopDesc)][
  merge.data.table(StopSums,FactFareDateCount,"StopID",all.x = T) %>%
    .[
      ,`:=`(
        Average_Boardings = round(Boarding/Days,1)
        ,Average_Alightings = round(Alighting/Days,1)
      )#end :=
      ]
  ,on = c(StopID = "StopID")
  ]

setcolorder(FF_postc_summary,c(1,2,4,6,3,7))

setnames(FF_postc_summary,names(FF_postc_summary),Stop_Ridership_col_names)

fwrite(FF_postc_summary,"data//FF_postc_summary.csv")


# compare pre and post ----------------------------------------------------

prec <- fread("data//FF_pc_summary.csv")

postc <- fread("data//FF_postc_summary.csv")
prec %>% setkey(`Stop Number`)
postc %>% setkey(`Stop Number`)

prec[postc[,-2]][
  ,on_change := `i.Average Daily On` - `Average Daily On`
  ][order(on_change)]


# now do sep thru march for service planning ADA --------------------------


# Import ------------------------------------------------------------------


# db connection

con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", Port = 1433)


# get applicable dates

DimServiceLevel <- tbl(con_DW,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")

DimDate_start_ADA <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "09/01/2019") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_end_ADA <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "03/14/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")


DimDate_full <- tbl(con_DW, "DimDate") %>%
  collect() %>% 
  setDT(key = "DateKey")

# get stops
DimStop <- tbl(con_DW,"DimStop") %>% 
  collect() %>% 
  setDT(key = "StopKey")

# get farekey

DimFare <- tbl(con_DW, "DimFare") %>%
  collect() %>%
  setDT(key = "FareKey")

# get Routes
DimRoute <- tbl(con_DW,"DimRoute") %>%
  filter() %>%
  collect() %>% 
  setDT(key = "RouteKey")

# get FF boarding and alighting

FactFare <- tbl(con_DW, "FactFare") %>%
  filter(
    FareKey %in% c(1001, 1002),
    DateKey >= local(DimDate_start_ADA$DateKey),
    DateKey <= local(DimDate_end_ADA$DateKey)
  ) %>% #end filter
  collect() %>% 
  setDT()
# Get Weekday only --------------------------------------------------------


#join servicelevel and a bit of stop info and dimfare while we're here

#mget for memory efficiency, keeps location the same
FactFare[
  #skinny stops first
  DimStop[,.(StopKey,StopID)]
  ,on = "StopKey"
  ,names(DimStop[,.(StopKey,StopID)]) := mget(paste0("i.",names(DimStop[,.(StopKey,StopID)])))
  ][
    #then service level skinny
    DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]
    ,on = "ServiceLevelKey"
    ,names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]) := mget(paste0("i.",names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)])))
    ][
      #then dimfareskinny
      DimFare[,.(FareKey,FareReportLabel)]
      , on = "FareKey"
      ,names(DimFare[,.(FareKey,FareReportLabel)]) := mget(paste0("i.",names(DimFare[,.(FareKey,FareReportLabel)])))
      ][,Service_Type := sub(".*-","",ServiceLevelReportLabel)
        ]



#check service
FactFare[,.(uniqueN(DateKey),.N),.(sub(".*-","",ServiceLevelReportLabel))]

FactFare[,.(uniqueN(DateKey),.N),Service_Type]


# count weekdays appearing for each stop ----------------------------------


#first we need the number of days each stop appears on weekdays only
FactFareDateCount <- 
  #filter for weekdays
  FactFare[Service_Type == "Weekday"][
    #get n date by stopid
    ,.(Days = uniqueN(DateKey))
    ,StopID
    ]

FactFare_joined_weekday <- FactFare[Service_Type == "Weekday"]
# get daily vehicle boards, then clean bad vehicles -----------------------


#get daily by vehicle
FF_joined_daily <- FactFare_joined_weekday[
  ,sum(FareCount)
  ,.(FareReportLabel,DateKey,VehicleKey)
  ][
    ,dcast(
      .SD
      ,DateKey + VehicleKey ~ FareReportLabel
      ,fill = 0
    )
    ]

#get zero days
FF_joined_zero <- FF_joined_daily[Boarding == 0 | Alighting == 0]

#remove them
FF_no_zero <- FactFare_joined_weekday[!FF_joined_zero,on = c(DateKey = "DateKey",VehicleKey = "VehicleKey")]

#graph it and check
FF_joined_daily[!FF_joined_zero,on = c(DateKey = "DateKey",VehicleKey = "VehicleKey")] %>%
  ggplot(aes(x=Boarding)) +
  geom_histogram(binwidth = 1) +
  stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)

#okay, 500 is a good bet i think
obvious_outlier <- 500

outlier_apc_vehicles <- FF_joined_daily[Boarding > obvious_outlier]

#remove outliers, get SD, then remove vehicles > three deeves and have b/a diff > 5%
three_deeves <- FF_no_zero[
  !outlier_apc_vehicles, on = c("DateKey","VehicleKey")
  ][
    ,sum(FareCount)
    ,.(FareReportLabel,DateKey,VehicleKey)
    ][
      ,dcast(
        .SD
        ,DateKey + VehicleKey ~ FareReportLabel
        ,fill = 0
      )
      ][,sd(Boarding)*3]+
  FF_no_zero[
    !outlier_apc_vehicles, on = c("DateKey","VehicleKey")
    ][
      ,sum(FareCount)
      ,.(FareReportLabel,DateKey,VehicleKey)
      ][
        ,dcast(
          .SD
          ,DateKey + VehicleKey ~ FareReportLabel
          ,fill = 0
        )
        ][,mean(Boarding)]

#add pct diff
FF_joined_daily[
  ,pct_diff := (Boarding - Alighting)/((Boarding + Alighting)/2)
  ]

#get deeve and big pct
FF_three_deeves_big_pct <- FF_joined_daily[(Boarding > three_deeves & pct_diff > 0.05) | 
                                             (Boarding > three_deeves & pct_diff < -0.1)]

FF_clean <- FF_no_zero[
  #remove three deeves
  !FF_three_deeves_big_pct, on = c("DateKey","VehicleKey")
  ]


# get boards by stop ------------------------------------------------------


#get daily sums by stopID
DailyStop <- FF_clean[
  #remove garage and 0
  StopID != 0 & StopID < 99000
  ,sum(FareCount)
  ,.(FareReportLabel,DateKey,StopID)
  ]

#get total boards for each stop
StopSums <- dcast(DailyStop[,sum(V1),.(StopID,FareReportLabel)],
                  StopID ~ FareReportLabel)


# get average daily boarding and alighting -------------------------------

#join stop sums with date count, then get rounded average
FF_ADA_summary <- DimStop[DimStop[,.I[StopKey == max(StopKey)],by = StopID]$V1][,.(StopID,StopDesc)][
  merge.data.table(StopSums,FactFareDateCount,"StopID",all.x = T) %>%
    .[
      ,`:=`(
        Average_Boardings = round(Boarding/Days,1)
        ,Average_Alightings = round(Alighting/Days,1)
      )#end :=
      ]
  ,on = c(StopID = "StopID")
  ]

setcolorder(FF_ADA_summary,c(1,2,4,6,3,7))

setnames(FF_ADA_summary,names(FF_ADA_summary),Stop_Ridership_col_names)

fwrite(FF_ADA_summary,"data//FF_ADA_summary.csv")


# compare pre and post ----------------------------------------------------

prec <- fread("data//FF_pc_summary.csv")

postc <- fread("data//FF_postc_summary.csv")

ada <- fread("data//FF_ADA_summary.csv")
ada %>% View()
prec %>% setkey(`Stop Number`)
postc %>% setkey(`Stop Number`)
ada %>% setkey(`Stop Number`)

ada[prec[,-2]][,on_change := `i.Average Daily On` - `Average Daily On`
               ][order(on_change)]


# get 2018 by stop by route -----------------------------------------------



# 2021/02/23 MPO ----------------------------------------------------------

library(dplyr)
library(data.table)
library(ggplot2)


# Import ------------------------------------------------------------------


#add Stop Ridership Col Names
Stop_Ridership_col_names <- c("Stop Number", "Stop Name", "Total Boardings", "Average Daily Boardings","Total Alightings","Average Daily Alightings","Days")


# Import ------------------------------------------------------------------


# db connection

con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", Port = 1433)


# get applicable dates

DimServiceLevel <- tbl(con_DW,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")

DimDate_start_MPO_pre <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "09/01/2019") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_end_MPO_pre <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "03/14/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")


DimDate_full <- tbl(con_DW, "DimDate") %>%
  collect() %>% 
  setDT(key = "DateKey")

# get stops
DimStop <- tbl(con_DW,"DimStop") %>% 
  collect() %>% 
  setDT(key = "StopKey")

# get farekey

DimFare <- tbl(con_DW, "DimFare") %>%
  collect() %>%
  setDT(key = "FareKey")

# get Routes
DimRoute <- tbl(con_DW,"DimRoute") %>%
  filter(RouteFareboxID %in% c("16","31","90","901","902")) %>%
  collect() %>% 
  setDT(key = "RouteKey")

# get FF boarding and alighting

FactFare_MPO_pre <- tbl(con_DW, "FactFare") %>%
  filter(
    FareKey %in% c(1001, 1002)
    ,DateKey >= local(DimDate_start_MPO_pre$DateKey)
    ,DateKey <= local(DimDate_end_MPO_pre$DateKey)
    ,RouteKey %in% local(DimRoute$RouteKey)
  ) %>% #end filter
  collect() %>% 
  setDT()

# detour ------------------------------------------------------------------


# bullshit <- tbl(con_DW,"FactFare") %>%
#   filter(FareKey %in% c(1001,1002)
#          ,DateKey == 8224
#          ,RouteKey %in% local(DimRoute$RouteKey)
#          ) %>%
#   collect() %>%
#     setDT()
  

# end detour --------------------------------------------------------------
# Get Weekday only --------------------------------------------------------


#join servicelevel and a bit of stop info and dimfare while we're here

#mget for memory efficiency, keeps location the same
FactFare_MPO_pre[
  #skinny stops first
  DimStop[,.(StopKey,StopID)]
  ,on = "StopKey"
  ,names(DimStop[,.(StopKey,StopID)]) := mget(paste0("i.",names(DimStop[,.(StopKey,StopID)])))
  ][
    #then service level skinny
    DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]
    ,on = "ServiceLevelKey"
    ,names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]) := mget(paste0("i.",names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)])))
    ][
      #then dimfareskinny
      DimFare[,.(FareKey,FareReportLabel)]
      , on = "FareKey"
      ,names(DimFare[,.(FareKey,FareReportLabel)]) := mget(paste0("i.",names(DimFare[,.(FareKey,FareReportLabel)])))
      ][
        #then dimroute
        DimRoute[,.(RouteKey,RouteReportLabel)]
        , on = "RouteKey"
        ,names(DimRoute[,.(RouteKey,RouteReportLabel)]) := mget(paste0("i.",names(DimRoute[,.(RouteKey,RouteReportLabel)])))
        ][,Service_Type := sub(".*-","",ServiceLevelReportLabel)
          ][
            DimDate_full
            , on = "DateKey"
            ,names(DimDate_full[,.(DateKey,CalendarDate)]) := mget(paste0("i.",names(DimDate_full[,.(DateKey,CalendarDate)])))
          ]



# date tests --------------------------------------------------------------


calendar <- seq.Date(as.IDate(DimDate_start_MPO_pre[,CalendarDate])
                     ,as.IDate(DimDate_end_MPO_pre[,CalendarDate])
                     ,"day")
all(
  calendar ==
    sort(
      unique(
        #throw your dataframe$datecolumn or whatever in here
        FactFare_MPO_pre[,unique(CalendarDate)]
        
      )#end unique
    )#end sort
)#end all

fsetdiff(data.table(calendar),FactFare_MPO_pre[,.(calendar = lubridate::as_date(CalendarDate))])


# transit day -------------------------------------------------------------

FactFare_MPO_pre[, c("ClockTime","Date") := list(as.ITime(stringr::str_sub(ServiceDateTime, 12, 19)),as.IDate(stringr::str_sub(ServiceDateTime, 1, 10)))
                 ][
                   #label prev day or current
                   , DateTest := ifelse(ClockTime<as.ITime("03:00:00"),1,0)
                   ][
                     , Transit_Day := fifelse(
                       DateTest ==1
                       ,lubridate::as_date(Date)-1
                       ,lubridate::as_date(Date)
                     )#end fifelse
                     ][
                       ,Transit_Day := lubridate::as_date("1970-01-01")+lubridate::days(Transit_Day)
                       ]




# weekdays appearing ------------------------------------------------------

FactFareDateCount <- 
  FactFare_MPO_pre[
    Service_Type == "Weekday"
  ][
    ,.(Days = uniqueN(Transit_Day))
    ,StopID
  ]

FactFare_MPO_pre[StopID == 90014,.N,Transit_Day]


FactFare_MPO_pre[
  Service_Type == "Weekday"
]

FactFare_MPO_pre[StopID == 90014][,.N,.(Transit_Day,Service_Type,RouteReportLabel)]
DimStop[StopID == 90014]

# get daily boards, then clean bad vehicles -------------------------------
FF_MPO_wkdy <- FactFare_MPO_pre[Service_Type == "Weekday"]

FF_MPO_pre_daily <- FF_MPO_wkdy[
  ,sum(FareCount)
  ,.(FareReportLabel,Transit_Day,VehicleKey)
][,dcast(
  .SD
  ,Transit_Day + VehicleKey ~ FareReportLabel
)]

#get zeros
FF_zero <- FF_MPO_pre_daily[Boarding == 0 | Alighting == 0]

#remove them
FF_no_zero <- FactFare_MPO_pre[!FF_zero,on = c(Transit_Day = "Transit_Day",VehicleKey = "VehicleKey")]


FF_MPO_pre_daily[!FF_zero,on = c(Transit_Day = "Transit_Day",VehicleKey = "VehicleKey")] %>%
  ggplot(aes(x=Boarding)) +
  geom_histogram(binwidth = 1) +
  stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)

outlier_apc_vehicles <- FF_MPO_pre_daily[Boarding > 1250]

three_deeves <- FF_no_zero[
  !outlier_apc_vehicles, on = c("Transit_Day","VehicleKey")
][
  ,sum(FareCount)
  ,.(
    FareReportLabel
    ,Transit_Day
    ,VehicleKey
  )
][
  ,dcast(
    .SD
    ,Transit_Day + VehicleKey ~ FareReportLabel
    ,fill = 0
  )
][,sd(Boarding)*3+mean(Boarding)]

FF_MPO_pre_daily[,pct_diff := (Boarding-Alighting)/((Boarding+Alighting)/2)]

FF_three_deeves_big_pct <- FF_MPO_pre_daily[
  Boarding > three_deeves & pct_diff > 0.1 |
    Boarding > three_deeves & pct_diff < -0.1
]

FF_MPO_pre_clean <- FF_no_zero[
  !FF_three_deeves_big_pct
  , on = c("Transit_Day" , "VehicleKey")
]


# get boards by stop ------------------------------------------------------

DailyStop <- FF_MPO_pre_clean[
  StopID != 0 & StopID < 99000
  ,sum(FareCount)
  ,.(FareReportLabel,Transit_Day,StopID)
]

StopSums <- dcast(DailyStop[,sum(V1),.(StopID,FareReportLabel)]
                  ,StopID ~ FareReportLabel)


# get average daily boarding/alighting ------------------------------------
FF_MPO_pre_summary <- DimStop[
  #get max StopKey row from DimStop so we have one single StopDesc
  DimStop[
    ,.I[
      StopKey == max(StopKey)
    ]
    , by = StopID
  ]$V1
][
  #select only stopid and stop desc
  ,.(StopID,StopDesc)
][
  #create data table of joined sums and date count, then use that to get avgs while joining to stopID,StopDesc
  merge.data.table(
    StopSums
    ,FactFareDateCount
    ,"StopID"
    ,all.x = T
  )[
    ,`:=`(
      Average_Boardings = round(Boarding/Days,1)
      ,Average_Alightings = round(Alighting/Days,1)
    )#end :=
  ]
  ,on = c(StopID = "StopID")
][!is.na(Days)]

setcolorder(FF_MPO_pre_summary, c(1,2,4,6,3,7))

setnames(FF_MPO_pre_summary,names(FF_MPO_pre_summary),Stop_Ridership_col_names)

fwrite(FF_MPO_pre_summary,"data//FF_MPO_pre_summary.csv")




FF_MPO_pre_clean[
  #first get daily sums
  ,sum(FareCount)
  ,.(FareReportLabel,Transit_Day,RouteReportLabel,StopID)
][,dcast(
  .SD
  ,Transit_Day + StopID + RouteReportLabel ~ FareReportLabel
)
][
  #then cut by week
  ,.(
    Boardings = sum(Boarding)
    ,Alightings = sum(Alighting)
    ,Number_of_Days = uniqueN(Transit_Day)
  )
  
  ,.(
    week_start = as.IDate(cut(Transit_Day,"week", start.on.monday = FALSE))
    ,StopID
    ,Route = RouteReportLabel
  )
][
  StopID != 0
][
  order(StopID,week_start)
][
 ,View(.SD) 
]


[
  ,.N
  ,Number_of_Days
]

# Import ------------------------------------------------------------------


#add Stop Ridership Col Names
Stop_Ridership_col_names <- c("Stop Number", "Stop Name", "Total Boardings", "Average Daily Boardings","Total Alightings","Average Daily Alightings","Days")



# do sep thru march for service planning ADA --------------------------


# Import ------------------------------------------------------------------


# db connection

con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", Port = 1433)


# get applicable dates

DimServiceLevel <- tbl(con_DW,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")

DimDate_start_MPO_post <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "09/01/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_end_MPO_post <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "02/01/2021") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_full <- tbl(con_DW, "DimDate") %>%
  collect() %>% 
  setDT(key = "DateKey")

# get stops
DimStop <- tbl(con_DW,"DimStop") %>% 
  collect() %>% 
  setDT(key = "StopKey")

# get farekey

DimFare <- tbl(con_DW, "DimFare") %>%
  collect() %>%
  setDT(key = "FareKey")

# get Routes
DimRoute <- tbl(con_DW,"DimRoute") %>%
  filter(RouteFareboxID %in% c("16","31","90","901","902")) %>%
  collect() %>% 
  setDT(key = "RouteKey")

# get FF boarding and alighting

FactFare_MPO_post <- tbl(con_DW, "FactFare") %>%
  filter(
    FareKey %in% c(1001, 1002)
    ,DateKey >= local(DimDate_start_MPO_post$DateKey)
    ,DateKey <= local(DimDate_end_MPO_post$DateKey)
    ,RouteKey %in% local(DimRoute$RouteKey)
  ) %>% #end filter
  collect() %>% 
  setDT()

# detour ------------------------------------------------------------------


# bullshit <- tbl(con_DW,"FactFare") %>%
#   filter(FareKey %in% c(1001,1002)
#          ,DateKey == 8224
#          ,RouteKey %in% local(DimRoute$RouteKey)
#          ) %>%
#   collect() %>%
#     setDT()


# end detour --------------------------------------------------------------
# Get Weekday only --------------------------------------------------------


#join servicelevel and a bit of stop info and dimfare while we're here

#mget for memory efficiency, keeps location the same
FactFare_MPO_post[
  #skinny stops first
  DimStop[,.(StopKey,StopID)]
  ,on = "StopKey"
  ,names(DimStop[,.(StopKey,StopID)]) := mget(paste0("i.",names(DimStop[,.(StopKey,StopID)])))
  ][
    #then service level skinny
    DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]
    ,on = "ServiceLevelKey"
    ,names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]) := mget(paste0("i.",names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)])))
    ][
      #then dimfareskinny
      DimFare[,.(FareKey,FareReportLabel)]
      , on = "FareKey"
      ,names(DimFare[,.(FareKey,FareReportLabel)]) := mget(paste0("i.",names(DimFare[,.(FareKey,FareReportLabel)])))
      ][
        #then dimroute
        DimRoute[,.(RouteKey,RouteReportLabel)]
        , on = "RouteKey"
        ,names(DimRoute[,.(RouteKey,RouteReportLabel)]) := mget(paste0("i.",names(DimRoute[,.(RouteKey,RouteReportLabel)])))
        ][,Service_Type := sub(".*-","",ServiceLevelReportLabel)
          ][
            DimDate_full
            , on = "DateKey"
            ,names(DimDate_full[,.(DateKey,CalendarDate)]) := mget(paste0("i.",names(DimDate_full[,.(DateKey,CalendarDate)])))
            ]



# date tests --------------------------------------------------------------


calendar <- seq.Date(as.IDate(DimDate_start_MPO_post[,CalendarDate])
                     ,as.IDate(DimDate_end_MPO_post[,CalendarDate])
                     ,"day")
all(
  calendar ==
    sort(
      unique(
        #throw your dataframe$datecolumn or whatever in here
        FactFare_MPO_post[,unique(CalendarDate)]
        
      )#end unique
    )#end sort
)#end all

fsetdiff(data.table(calendar),FactFare_MPO_post[,.(calendar = lubridate::as_date(CalendarDate))])


# transit day -------------------------------------------------------------

FactFare_MPO_post[, c("ClockTime","Date") := list(as.ITime(stringr::str_sub(ServiceDateTime, 12, 19)),as.IDate(stringr::str_sub(ServiceDateTime, 1, 10)))
                 ][
                   #label prev day or current
                   , DateTest := ifelse(ClockTime<as.ITime("03:00:00"),1,0)
                   ][
                     , Transit_Day := fifelse(
                       DateTest ==1
                       ,lubridate::as_date(Date)-1
                       ,lubridate::as_date(Date)
                     )#end fifelse
                     ][
                       ,Transit_Day := lubridate::as_date("1970-01-01")+lubridate::days(Transit_Day)
                       ]




# weekdays appearing ------------------------------------------------------

FactFareDateCount <- 
  FactFare_MPO_post[
    Service_Type == "Weekday"
    ][
      ,.(Days = uniqueN(Transit_Day))
      ,StopID
      ]



# get daily boards, then clean bad vehicles -------------------------------
FF_MPO_wkdy <- FactFare_MPO_post[Service_Type == "Weekday"]

FF_MPO_post_daily <- FF_MPO_wkdy[
  ,sum(FareCount)
  ,.(FareReportLabel,Transit_Day,VehicleKey)
  ][,dcast(
    .SD
    ,Transit_Day + VehicleKey ~ FareReportLabel
  )]

#get zeros
FF_zero <- FF_MPO_post_daily[Boarding == 0 | Alighting == 0]

#remove them
FF_no_zero <- FactFare_MPO_post[!FF_zero,on = c(Transit_Day = "Transit_Day",VehicleKey = "VehicleKey")]


FF_MPO_post_daily[!FF_zero,on = c(Transit_Day = "Transit_Day",VehicleKey = "VehicleKey")] %>%
  ggplot(aes(x=Boarding)) +
  geom_histogram(binwidth = 1) +
  stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)

outlier_apc_vehicles <- FF_MPO_post_daily[Boarding > 1250]

three_deeves <- FF_no_zero[
  !outlier_apc_vehicles, on = c("Transit_Day","VehicleKey")
  ][
    ,sum(FareCount)
    ,.(
      FareReportLabel
      ,Transit_Day
      ,VehicleKey
    )
    ][
      ,dcast(
        .SD
        ,Transit_Day + VehicleKey ~ FareReportLabel
        ,fill = 0
      )
      ][,sd(Boarding)*3+mean(Boarding)]

FF_MPO_post_daily[,pct_diff := (Boarding-Alighting)/((Boarding+Alighting)/2)]

FF_three_deeves_big_pct <- FF_MPO_post_daily[
  Boarding > three_deeves & pct_diff > 0.1 |
    Boarding > three_deeves & pct_diff < -0.1
  ]

FF_MPO_post_clean <- FF_no_zero[
  !FF_three_deeves_big_pct
  , on = c("Transit_Day" , "VehicleKey")
  ]


# get boards by stop ------------------------------------------------------

DailyStop <- FF_MPO_post_clean[
  StopID != 0 & StopID < 99000
  ,sum(FareCount)
  ,.(FareReportLabel,Transit_Day,StopID)
  ]

StopSums <- dcast(DailyStop[,sum(V1),.(StopID,FareReportLabel)]
                  ,StopID ~ FareReportLabel)


# get average daily boarding/alighting ------------------------------------
FF_MPO_post_summary <- DimStop[
  #get max StopKey row from DimStop so we have one single StopDesc
  DimStop[
    ,.I[
      StopKey == max(StopKey)
      ]
    , by = StopID
    ]$V1
  ][
    #select only stopid and stop desc
    ,.(StopID,StopDesc)
    ][
      #create data table of joined sums and date count, then use that to get avgs while joining to stopID,StopDesc
      merge.data.table(
        StopSums
        ,FactFareDateCount
        ,"StopID"
        ,all.x = T
      )[
        ,`:=`(
          Average_Boardings = round(Boarding/Days,1)
          ,Average_Alightings = round(Alighting/Days,1)
        )#end :=
        ]
      ,on = c(StopID = "StopID")
      ][!is.na(Days)]

setcolorder(FF_MPO_post_summary, c(1,2,4,6,3,7))

setnames(FF_MPO_post_summary,names(FF_MPO_post_summary),Stop_Ridership_col_names)

fwrite(FF_MPO_post_summary,"data//FF_MPO_post_summary.csv")

