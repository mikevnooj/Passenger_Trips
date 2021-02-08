# 6/26/20 - station activity for service planning
#two date ranges, Jan - MArch 14th
#june - Oct 1
library(dplyr)
library(data.table)
library(ggplot2)


# Import ------------------------------------------------------------------


#add Stop Ridership Col Names
Stop_Ridership_col_names <- c("Stop Number", "Stop Name", "Passengers On", "Average Daily On","Passengers Off","Average Daily Off","Days")



# do sep thru march for service planning ADA --------------------------


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
      ][
        #then dimroute
        DimRoute[,.(RouteKey,RouteReportLabel)]
        , on = "RouteKey"
        ,names(DimRoute[,.(RouteKey,RouteReportLabel)]) := mget(paste0("i.",names(DimRoute[,.(RouteKey,RouteReportLabel)])))
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
  ][!RouteReportLabel %in% c("Deadhead","(X) Undefined")]


# get boards by stop and route ------------------------------------------------------


#get daily sums by stopID
DailyStop <- FF_clean[
  #remove garage and 0
  StopID != 0 & StopID < 99000
  ,sum(FareCount)
  ,.(FareReportLabel,DateKey,StopID,RouteReportLabel)
  ]

#get total boards for each stop
StopSums <- dcast(DailyStop[,sum(V1),.(StopID,FareReportLabel,RouteReportLabel)],
                  StopID + RouteReportLabel ~ FareReportLabel)


# get average daily boarding and alighting -------------------------------

#join stop sums with date count, then get rounded average

Stops_To_Join <- DimStop[DimStop[,.I[StopKey == max(StopKey)],by = StopID]$V1][,.(StopID,StopDesc)]

Pre_Cov_1920_summary <- merge.data.table(StopSums,FactFareDateCount,"StopID",all.x = T)[
  ,`:=`(
    Average_Boardings = round(Boarding/Days,1)
    ,Average_Alightings = round(Alighting/Days,1)
  )#end :=
  ][
    Stops_To_Join
    ,on = "StopID"
    ,names(Stops_To_Join) := mget(paste0("i.",names(Stops_To_Join)))
    ]

fwrite(Pre_Cov_1920_summary,"data//NN_2019_2020_pre_covid.csv")


fwrite(x,"data//NN_Sep19_Thru_March20_by_Stop_by_Route")

identical(x,fread("data//NN_Sep19_Thru_March20_by_Stop_by_Route"))

# Detour ------------------------------------------------------------------
#what the fuck is happening to route 31 at the ctc
FF_clean[RouteReportLabel == 31 & StopID == 90009 & FareCount > 0 & FareReportLabel == "Alighting"]
FF_clean[RouteReportLabel == 31 & StopID %like% 900 & FareCount > 0 & FareReportLabel == "Alighting"]

FF_clean[RouteReportLabel == 14 & StopID == 90008 & FareKey == 1002 & FareCount > 0]

FF_clean[DateKey == 8010 & VehicleKey == 1218] %>%
  select(ServiceDateTime,RouteReportLabel,StopID,FareKey,FareCount) %>% View()
# end detour --------------------------------------------------------------


# FF_ADA_summary <- DimStop[DimStop[,.I[StopKey == max(StopKey)],by = StopID]$V1][,.(StopID,StopDesc)][
#   merge.data.table(StopSums,FactFareDateCount,"StopID",all.x = T)[
#       ,`:=`(
#         Average_Boardings = round(Boarding/Days,1)
#         ,Average_Alightings = round(Alighting/Days,1)
#       )#end :=
#       ][]
#   ,on = c(StopID = "StopID")
#   ]






setcolorder(FF_ADA_summary,c(1,2,4,6,3,7))

setnames(FF_ADA_summary,names(FF_ADA_summary),Stop_Ridership_col_names)

#fwrite(FF_ADA_summary,"data//FF_ADA_summary.csv")


# Get boardings from TMDM then Avail --------------------------------------



# Get daily boardings at DTC from TMDM, then get from Avail...

### --- Database connection --- ###

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "IPTC-TMDATAMART\\TMDATAMART",
                      Database = "TMDATAMART", Port = 1433)

### --- Data Import --- ###

# get routes

ROUTE_raw <- tbl(con, "ROUTE") %>% 
  select(ROUTE_ID, ROUTE_ABBR) %>%
  collect() %>%
  data.table(key = "ROUTE_ID")


# get stops

GEO_NODE_raw <- tbl(con, "GEO_NODE") %>% 
  select(GEO_NODE_ID, GEO_NODE_ABBR, GEO_NODE_NAME) %>%
  collect() %>% 
  data.table(key = "GEO_NODE_ID")

# get calendar

CALENDAR_raw <- tbl(con, "CALENDAR") %>% 
  select(CALENDAR_ID, CALENDAR_DATE) %>%
  collect() %>%
  data.table(key = "CALENDAR_ID")

SERVICE_SELECTION_raw <- tbl(con,"SERVICE_SELECTION") %>%
  collect() %>%
  data.table()

SERVICE_TYPE_raw <- tbl(con,"SERVICE_TYPE") %>%
  collect() %>%
  data.table(key = "SERVICE_TYPE_ID")

# get set of GEO_NODE_ID for CTC

SERVICE_SELECTION_raw[CALENDAR_ID %like% 12019][order(CALENDAR_ID)]

# set query

PASSENGER_COUNT_query <- tbl(con, sql("SELECT CALENDAR_ID, ROUTE_ID, GEO_NODE_ID, ARRIVAL_TIME, DEPARTURE_LOAD,
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
                                      "))

#fwrite(PASSENGER_COUNT_raw,"data//Pass_Count_Raw.csv")

PASSENGER_COUNT_raw <- fread("data//Pass_Count_Raw.csv")




# Get Weekday Only --------------------------------------------------------

#do weekdays
PASSENGER_COUNT_raw_weekday <- PASSENGER_COUNT_raw[
  SERVICE_SELECTION_raw
  ,on = "CALENDAR_ID"
  ,names(SERVICE_SELECTION_raw) := mget(paste0("i.",names(SERVICE_SELECTION_raw)))
][
  #filter for weekday service == 3
  SERVICE_TYPE_ID == 3
]


PASSENGER_COUNT_raw_weekday[
  #join calendar
  CALENDAR_raw
  ,on = "CALENDAR_ID"
  ,names(CALENDAR_raw) := mget(paste0("i.",names(CALENDAR_raw)))
][
  #join geonode
  GEO_NODE_raw
  ,on = "GEO_NODE_ID"
  ,names(GEO_NODE_raw) := mget(paste0("i.",names(GEO_NODE_raw)))
][
  #join route
  ROUTE_raw
  ,on = "ROUTE_ID"
  ,names(ROUTE_raw) := mget(paste0("i.",names(ROUTE_raw)))
]


# count weekdays appearing for each stop ----------------------------------
DateCount <- PASSENGER_COUNT_raw_weekday[
  ,.(Days = uniqueN(CALENDAR_DATE))
  ,.(GEO_NODE_ABBR)
]

# get boards and clean bad vehicles ---------------------------------------

#get daily by vehicle
PASSENGER_COUNT_joined_daily <- PASSENGER_COUNT_raw_weekday[
  ,.(
    Boardings = sum(BOARD)
    ,Alightings = sum(ALIGHT)
  )
  ,.(CALENDAR_ID,VEHICLE_ID)
]


#get zero vehicle days

PC_joined_zero <- PASSENGER_COUNT_joined_daily[
  Boardings == 0 | Alightings == 0
]

#remove them
PC_no_zero <- PASSENGER_COUNT_raw_weekday[!PC_joined_zero,on = c(CALENDAR_ID = "CALENDAR_ID",VEHICLE_ID = 'VEHICLE_ID')]

#graph it and check
PASSENGER_COUNT_joined_daily[!PC_joined_zero,on = c(CALENDAR_ID = "CALENDAR_ID",VEHICLE_ID = 'VEHICLE_ID')] %>% 
  ggplot(aes(x=Boardings)) + 
  geom_histogram(binwidth = 1) +
  stat_bin(binwidth = 1,geom = "text",aes(label = ..x..),vjust = -1.5)
  
#eh, we'll leave it and see


# get boards by stop and route ------------------------------------------------------

PC_Daily_Stop <- PC_no_zero[
  #remove garage and 0
  GEO_NODE_ABBR !=0 & GEO_NODE_ABBR < 99000
  ,#get our boards and alights
  .(
    Boardings = sum(BOARD)
    ,Alightings = sum(ALIGHT)
  )
  ,#group by day and stop and route
  .(
    CALENDAR_ID
    ,GEO_NODE_ABBR
    ,ROUTE_ABBR
  )
]

PC_Stop_Sums <- PC_Daily_Stop[
  ,.(
    Boardings = sum(Boardings)
    ,Alightings = sum(Alightings)
  )
  ,.(GEO_NODE_ABBR
     ,ROUTE_ABBR)
]

# detour ------------------------------------------------------------------

#what the fuck is this million alighting bullshit
PC_Stop_Sums[GEO_NODE_ABBR == 90009]

#let's see
PC_no_zero[ROUTE_ABBR == 12 & GEO_NODE_ABBR == 90009]

#no idea
# detour ------------------------------------------------------------------
stops_to_check <- c(10055
                    ,10058
                    ,10199
                    ,10200
                    ,12568
                    ,50529
                    ,50601
                    ,51259
                    
)

PC_Daily_Stop[GEO_NODE_ABBR %in% stops_to_check][,max(CALENDAR_ID),GEO_NODE_ABBR]

# detour ------------------------------------------------------------------


# get avg daily boarding and alighting ------------------------------------

PC_route_stop_summary <- merge.data.table(PC_Stop_Sums,DateCount,all.x = T)[
    ,`:=`(
      Average_Boardings = round(Boardings/Days,1)
      ,Average_Alightings = round(Alightings/Days,1)
    )
  ][
    GEO_NODE_raw[,.(GEO_NODE_ABBR,GEO_NODE_NAME)]
    ,on = "GEO_NODE_ABBR"
    ,names(GEO_NODE_raw[,.(GEO_NODE_ABBR,GEO_NODE_NAME)]) := mget(paste0("i.",names(GEO_NODE_raw[,.(GEO_NODE_ABBR,GEO_NODE_NAME)])))
    ]

PC_route_stop_summary[]

#fix the names and col order
setcolorder(PC_route_stop_summary,c(1,2,4,3,5,6,7,8))


setnames(PC_route_stop_summary,c("StopID","RouteReportLabel","Alightings","Boardings","Days","Average_Boardings","Average_Alightings","StopDesc"))


fwrite(PC_route_stop_summary,"data//NN_2018_by_Stop_by_Route.csv")



# detour ------------------------------------------------------------------

#check that each stop has the right amount of days
PASSENGER_COUNT_raw %>%
  group_by(GEO_NODE_ID) %>%
  summarise(ndays = n_distinct(CALENDAR_ID))

#uhhhhhhh

# end detour --------------------------------------------------------------



# now do post-cov by route by stop ----------------------------------------

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


eight <- fread("data//NN_2018_by_Stop_by_Route.csv")
nine <- fread("data//NN_Sep19_Thru_March20_by_Stop_by_Route.csv")
twenty <-fread("data//NN_Mar20_thru_Oct20_by_Stop_by_Route.csv")

test_this_is_post_cov <- fread("data//test.csv")

which_one <- fread("data//NN_March_Thru_September_by_Stop_by_Route.csv")


identical(eight,nine)
