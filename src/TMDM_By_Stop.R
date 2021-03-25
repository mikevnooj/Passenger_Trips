# Samuel Day-Woodruff
# Data Analyst, IndyGo
# May 7, 2019

### --- Purpose --- ###

# This script collects monthly boardings by stop for the past twelve months
# from TMDM and Avail sources.

### --- Notes --- ###

# Get TMDM first, then get Avail,.

### --- Revision History --- ###



### --- Libraries --- ###

library(tidyverse)

### --- Database connection --- ###

# con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
#                       Database = "TransitAuthority_IndyGo", PWD = rstudioapi::askForPassword("Database password"), 
#                       Port = 1433)
# 
# # or
# 
# con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
#                        Database = "DW_IndyGo", PWD = rstudioapi::askForPassword("Database password"), 
#                        Port = 1433)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "IPTC-TMDATAMART\\TMDATAMART", 
                      Database = "TMDATAMART", PWD = rstudioapi::askForPassword("Database password"), 
                      Port = 1433)

### --- Queries --- ###

# get routes

ROUTE_raw <- tbl(con, "ROUTE") %>% 
  select(ROUTE_ID, ROUTE_ABBR) %>%
  collect()

# get stops

GEO_NODE_raw <- tbl(con, "GEO_NODE") %>% 
  select(GEO_NODE_ID, GEO_NODE_ABBR, GEO_NODE_NAME, LATITUDE, LONGITUDE) %>%
  collect()

# get calendar

CALENDAR_raw <- tbl(con, "CALENDAR") %>% 
  select(CALENDAR_ID, CALENDAR_DATE) %>%
  collect()

# get passenger counts

PASSENGER_COUNT_query <- tbl(con, sql("SELECT CALENDAR_ID
, ROUTE_ID
, GEO_NODE_ID
, ARRIVAL_TIME
, DEPARTURE_LOAD,
BOARD, ALIGHT, VEHICLE_ID, TRIP_ID, ROUTE_DIRECTION_ID, BLOCK_STOP_ORDER
                                      from PASSENGER_COUNT
                                      WHERE (CALENDAR_ID >= 120180101.0) and (CALENDAR_ID < 120190101.0)
                                      and PASSENGER_COUNT.TRIP_ID IS NOT NULL
                                      and PASSENGER_COUNT.VEHICLE_ID IN (SELECT dbo.SCHEDULE.VEHICLE_ID
                                      FROM dbo.SCHEDULE with (nolock) WHERE PASSENGER_COUNT.CALENDAR_ID = dbo.SCHEDULE.CALENDAR_ID
                                      AND PASSENGER_COUNT.TIME_TABLE_VERSION_ID=dbo.SCHEDULE.TIME_TABLE_VERSION_ID
                                      AND PASSENGER_COUNT.ROUTE_ID = dbo.SCHEDULE.ROUTE_ID
                                      AND PASSENGER_COUNT.ROUTE_DIRECTION_ID = dbo.SCHEDULE.ROUTE_DIRECTION_ID
                                      AND PASSENGER_COUNT.TRIP_ID = dbo.SCHEDULE.TRIP_ID
                                      AND PASSENGER_COUNT.GEO_NODE_ID = dbo.SCHEDULE.GEO_NODE_ID)
                                      ")) # may need to just join and do summary...

PASSENGER_COUNT_raw <- PASSENGER_COUNT_query %>% collect()

### --- Cleaning / Joining --- ###

# Add calendar_date, GEO_NODE_ABBR

PASSENGER_COUNT_raw <- PASSENGER_COUNT_raw %>%
  left_join(CALENDAR_raw, by = "CALENDAR_ID") %>%
  left_join(GEO_NODE_raw, by = "GEO_NODE_ID") 

# Add month.

PASSENGER_COUNT_raw$month <- lubridate::month(PASSENGER_COUNT_raw$CALENDAR_DATE,
                                              label = TRUE)

PASSENGER_COUNT_raw$year <- lubridate::year(PASSENGER_COUNT_raw$CALENDAR_DATE)

PASSENGER_COUNT_raw <- PASSENGER_COUNT_raw %>%
  mutate(Month_Year = format(CALENDAR_DATE, "%Y-%m"))

# fix geo_node lat/lon

PASSENGER_COUNT_raw$LATITUDE <- PASSENGER_COUNT_raw$LATITUDE / 10000000
PASSENGER_COUNT_raw$LONGITUDE <- PASSENGER_COUNT_raw$LONGITUDE / 10000000

GEO_NODE_raw$LATITUDE <- GEO_NODE_raw$LATITUDE / 10000000
GEO_NODE_raw$LONGITUDE <- GEO_NODE_raw$LONGITUDE / 10000000

### --- Analysis --- ###

# Get ridership by stop

ridership_by_stop <- PASSENGER_COUNT_raw %>%
  group_by(GEO_NODE_ABBR, Month_Year) %>%
  summarise(monthly_boardings = sum(BOARD)) 

ridership_by_stop_joined <- ridership_by_stop %>%
  left_join(select(GEO_NODE_raw, everything()),
            by = "GEO_NODE_ABBR") %>%
  select(everything(), -GEO_NODE_ID, 
         Public_ID = GEO_NODE_ABBR,
         Public_Stop_Name = GEO_NODE_NAME)

View(ridership_by_stop_joined)

head(PASSENGER_COUNT_raw) %>% view()
ndays <- PASSENGER_COUNT_raw %>%
  group_by(GEO_NODE_ABBR)


##### now do the same for Avail data #####

### --- Database connections --- ###

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                       Database = "DW_IndyGO", PWD = rstudioapi::askForPassword("Database password"), 
                       Port = 1433)

### --- Data Import --- ###

## Get some Fact tables re: boardings

# get stops

DimStop <- tbl(con2, "DimStop") %>%
  collect()

# get boardings

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo", PWD = rstudioapi::askForPassword("Database password"), 
                      Port = 1433)

Apc_Data_raw<- tbl(con, dbplyr::in_schema("avl", "Apc_Data")) %>%
  collect()

# let's extract dates

Apc_Data_raw$Date <- as.Date(str_sub(Apc_Data_raw$GPS_Time, 1, 10))

Apc_Data_raw$Clock_Time <- str_sub(Apc_Data_raw$GPS_Time, 12, 19)


Apc_Data_raw$DateTest <- ifelse(Apc_Data_raw$Clock_Time < 
                                  "03:00:00", 1, 0)

Apc_Data_raw$Transit_Day_Unix <- ifelse(Apc_Data_raw$DateTest == 1,
                                        lubridate::as_date(Apc_Data_raw$Date - 1),
                                        Apc_Data_raw$Date)

Apc_Data_raw$Epoch_Date <- as.Date("1970-01-01")

Apc_Data_raw$Transit_Day <- Apc_Data_raw$Epoch_Date + lubridate::days(Apc_Data_raw$Transit_Day_Unix)

Apc_Data_raw <- Apc_Data_raw %>%
  mutate(Month_Year = format(Transit_Day, "%Y-%m"))

# get rid of test vehicles

# More importantly... how do I know ANY of the counts are right?

# ridership_by_stop_joined <- ridership_by_stop%>% # (this needs to be corrected for DimStop)
#   left_join(select(DimStop, everything()),
#             by = "GEO_NODE_ABBR") %>%
#   select(everything(), -GEO_NODE_ID, 
#          Public_ID = GEO_NODE_ABBR,
#          Public_Stop_Name = GEO_NODE_NAME)

# (Ignore these issues for now)

ridership_by_stop_Avail <- Apc_Data_raw %>%
  group_by(Stop, Month_Year) %>%
  summarise(monthly_boardings = sum(Boarding))

# that looks like a LOT of unallocated stops. What percentage of stops are unallocated?

sum(ridership_by_stop_Avail[ridership_by_stop_Avail$Stop == 0,3]) / sum(ridership_by_stop_Avail$monthly_boardings)

# so, about 10 % of stops are unallocated...

# WTF do I do about that.

# Let's compare these numbers to dimension table numbers.

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", 
                       Server = "AVAILDWHP01VW", Database = "DW_IndyGO", PWD = rstudioapi::askForPassword("Database password"), 
                       Port = 1433) 

Other_Boarding_Method_departing_stop <- tbl(con2, "FactSegmentActual") %>%
  left_join(tbl(con2, "DimDate"), by = "DateKey") %>%
  left_join(tbl(con2, "DimStop"),
            by = c("DepartStopKey" = "StopKey")) %>%
  left_join(tbl(con2, "DimStop"),
            by = c("ArriveStopKey" = "StopKey")) %>%
  group_by(StopID.x, CalendarDate) %>%
  summarise(DepartBoard = sum(DepartBoards, na.rm = TRUE),
            DepartAlight = sum(DepartAlights, na.rm = TRUE),
            ArriveBoard = sum(ArriveBoards, na.rm = TRUE),
            ArriveAlight = sum(ArriveAlights, na.rm = TRUE)) %>%
  collect()

Other_Boarding_Method_departing_stop <- Other_Boarding_Method_departing_stop %>%
  mutate(Month_Year = format(CalendarDate, "%Y-%m")) %>%
  group_by(StopID.x, Month_Year) %>%
  summarise(DepartBoard = sum(DepartBoard, na.rm = TRUE),
            DepartAlight = sum(DepartAlight, na.rm = TRUE),
            ArriveBoard = sum(ArriveBoard, na.rm = TRUE),
            ArriveAlight = sum(ArriveAlight, na.rm = TRUE))

Other_Boarding_Method_arriving_stop <- tbl(con2, "FactSegmentActual") %>%
  left_join(tbl(con2, "DimDate"), by = "DateKey") %>%
  left_join(tbl(con2, "DimStop"),
            by = c("DepartStopKey" = "StopKey")) %>%
  left_join(tbl(con2, "DimStop"),
            by = c("ArriveStopKey" = "StopKey")) %>%
  group_by(StopID.y, CalendarDate) %>%
  summarise(DepartBoard = sum(DepartBoards, na.rm = TRUE),
            DepartAlight = sum(DepartAlights, na.rm = TRUE),
            ArriveBoard = sum(ArriveBoards, na.rm = TRUE),
            ArriveAlight = sum(ArriveAlights, na.rm = TRUE)) %>%
  collect()

Other_Boarding_Method_arriving_stop <- Other_Boarding_Method_arriving_stop %>%
  mutate(Month_Year = format(CalendarDate, "%Y-%m")) %>%
  group_by(StopID.y, Month_Year) %>%
  summarise(DepartBoard = sum(DepartBoard, na.rm = TRUE),
            DepartAlight = sum(DepartAlight, na.rm = TRUE),
            ArriveBoard = sum(ArriveBoard, na.rm = TRUE),
            ArriveAlight = sum(ArriveAlight, na.rm = TRUE))

View(Other_Boarding_Method_departing_stop)
View(Other_Boarding_Method_arriving_stop)

sum(Other_Boarding_Method_departing_stop$DepartBoard)
sum(Other_Boarding_Method_departing_stop$ArriveBoard)

sum(Other_Boarding_Method_arriving_stop$DepartBoard)
sum(Other_Boarding_Method_arriving_stop$ArriveBoard, na.rm = TRUE)

Other_Boarding_Method_sample <- tbl(con2, "FactSegmentActual") %>%
  left_join(tbl(con2, "DimDate"), by = "DateKey") %>%
  left_join(tbl(con2, "DimStop"),
            by = c("DepartStopKey" = "StopKey")) %>%
  left_join(tbl(con2, "DimStop"),
            by = c("ArriveStopKey" = "StopKey")) %>%
  head() %>%
  collect()

sum(ridership_by_stop_Avail$monthly_boardings)

# WTF. how can there be such a difference between the two. 

sum(ridership_by_stop_Avail[ridership_by_stop_Avail$Stop == 0,3]) + 
  sum(Other_Boarding_Method_departing_stop$DepartBoard)+
  sum(Other_Boarding_Method_departing_stop$ArriveBoard)

# ... omg.
# check date range of these two to make sure I'm not getting skinny data.

range(Other_Boarding_Method_departing_stop$CalendarDate) # Oct-Apr. (how much april?)
range(ridership_by_stop_Avail$Month_Year) # this goes some september to 

FactSegmentActual_range_hack <- tbl(con2, "FactSegmentActual") %>%
  left_join(tbl(con2, "DimDate"), by = "DateKey") %>%
  select(CalendarDate) %>%
  distinct(CalendarDate) %>%
  collect()

range(FactSegmentActual_range_hack$CalendarDate)

# wow! ok! so the issue was that i was connecting to the wrong db! (kind)

# let's use departing stops. (Other_Boarding_Method_departing_stop$DepartBoard)
# need some final cleaning before join.

# convert character to integer (could lead to issues though.)

ridership_by_stop_joined$Public_ID <- as.integer(ridership_by_stop_joined$Public_ID)

# convert date formats

ridership_by_stop_joined_scraps <- ridership_by_stop_joined %>%
  anti_join(select(Other_Boarding_Method_departing_stop, StopID.x, Month_Year,
                   DepartBoard),
            by = c("Public_ID" = "StopID.x", "Month_Year"))

ridership_by_stop_joined_joined <- ridership_by_stop_joined %>%
  left_join(select(Other_Boarding_Method_departing_stop, StopID.x, Month_Year,
                   DepartBoard),
            by = c("Public_ID" = "StopID.x", "Month_Year"))

ridership_by_stop_joined_joined[is.na(ridership_by_stop_joined_joined$DepartBoard)] <- 0

ridership_by_stop_joined_joined$Monthly_Boardings <- ridership_by_stop_joined_joined$monthly_boardings +
  ridership_by_stop_joined_joined$DepartBoard

data_to_export <- select(ridership_by_stop_joined_joined, everything(),
                         Year_Month = Month_Year,
                         -monthly_boardings, -DepartBoard)

View(data_to_export)

# write.csv(data_to_export, file = "Monthly_Boardings_by_Stop_0518_0419.csv",
#           row.names = FALSE)
