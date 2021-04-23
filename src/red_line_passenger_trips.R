# Red Line Conversion to DT

#Feb 9 is when the switch happened and 901/902 had their own trips

library(data.table)
library(leaflet)
library(dplyr)
library(magrittr)
library(stringr)
library(timeDate)
library(lubridate)
library(ggplot2)

# Database Connections ----------------------------------------------------

con_rep <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW\\REPSQLP02", 
                          Database = "TransitAuthority_IndyGo_Reporting", 
                          Port = 1433)

# Time --------------------------------------------------------------------

this_month_Avail <- lubridate::floor_date(Sys.Date(), unit = "month")

last_month_Avail  <- lubridate::floor_date(lubridate::floor_date(Sys.Date()
                                                                 ,unit = "month"
                                                                 ) - 1 #end floor_date
                                           ,unit = "month"
                                           )#end floor_date

month <- "2020-03-01"

this_month_Avail <- lubridate::floor_date(x = lubridate::as_date(month)
                                         , unit = "month"
                                         )

last_month_Avail <- lubridate::floor_date(lubridate::floor_date(lubridate::as_date(month)
                                                               , unit = "month"
                                                               ) - 1 #end lubridate::floor_date
                                         ,unit = "month"
                                         )#end lubridate::floor_date







# this_month_Avail_GPS_search <- as.POSIXct(this_month_Avail) + 111600
# 
# last_month_Avail_GPS_search <- as.POSIXct(last_month_Avail) - 234000

VMH_StartTime <- str_remove_all(last_month_Avail,"-")

VMH_EndTime <- str_remove_all(this_month_Avail,"-")



#paste0 the query
VMH_Raw <- tbl(
  con_rep,sql(
    paste0(
      "select a.Time
    ,a.Route
    ,Boards
    ,Alights
    ,Trip
    ,Vehicle_ID
    ,Stop_Name
    ,Stop_Id
    ,Inbound_Outbound
    ,Departure_Time
    ,Latitude
    ,Longitude
    ,GPSStatus
    ,CommStatus
    ,a.Avl_History_Id
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Route like '90%'
    and a.Time > '",VMH_StartTime,"'
    and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
    
    UNION
    
    select a.Time
    ,a.Route
    ,Boards
    ,Alights
    ,Trip
    ,Vehicle_ID
    ,Stop_Name
    ,Stop_Id
    ,Inbound_Outbound
    ,Departure_Time
    ,Latitude
    ,Longitude
    ,GPSStatus
    ,CommStatus
    ,a.Avl_History_Id
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Time > '",VMH_StartTime,"'
    and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
    and Vehicle_ID > 1950
    and Vehicle_ID < 2000
    
    UNION
    
      select a.Time
      ,a.Route
      ,Boards
      ,Alights
      ,Trip
      ,Vehicle_ID
      ,Stop_Name
      ,Stop_Id
      ,Inbound_Outbound
      ,Departure_Time
      ,Latitude
      ,Longitude
      ,GPSStatus
      ,CommStatus
      ,a.Avl_History_Id
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Time > '",VMH_StartTime,"'
      and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
      and Vehicle_ID = 1899"
      )#end paste
  )#endsql
) %>% #end tbl
  collect() %>%
  data.table()


# Cleaning ----------------------------------------------------------------

#get 90 and remove garage boardings
VMH_Raw_90 <- VMH_Raw[Latitude > 39.709468 & Latitude < 39.877512
                      ][Longitude > -86.173321]

#do transit day

Transit_Day_Cutoff <- as.ITime("03:30:00")

VMH_Raw_90[
  , DateTest := fifelse(data.table::as.ITime(Time) < Transit_Day_Cutoff
                        , 1
                        , 0
                        ) #end fifelse()
  ][
    , Transit_Day := fifelse(DateTest == 1
                             ,data.table::as.IDate(Time)-1
                             ,data.table::as.IDate(Time)
                             )
    ]

VMH_Raw_90 <- VMH_Raw_90[Transit_Day >= last_month_Avail & Transit_Day < this_month_Avail]

#VMH_Raw_90[Vehicle_ID ==  1984,.(Boards = sum(Boards)),.(Day = Transit_Day)][order(Day)]

#add seconds since midnight for later

VMH_Raw_90[
  ,seconds_since_midnight := fifelse(Transit_Day == as.IDate(Time)
          ,as.numeric(as.ITime(Time) - as.ITime(Transit_Day))
          ,as.numeric(as.ITime(Time))+24*60*60)
][
  ,Clock_Hour := data.table::hour(Time)
]


#consider adding error catching here for times

# detour ------------------------------------------------------------------


VMH_Raw_90[Transit_Day != as.IDate(Time)][1]



#looks good

# end detour --------------------------------------------------------------


#confirm dates
VMH_Raw_90[,.N,Transit_Day][order(Transit_Day)]
#error catch here as well

# date comparo ------------------------------------------------------------
#left_join(by_day,by_day_sam) %>% mutate(diff = N-`n()`)

# end date comparo --------------------------------------------------------

#set service type
holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

#set sat sun
holidays_sunday <- holiday(2000:2025, holidays_sunday_service)
holidays_saturday <- holiday(2000:2025, holidays_saturday_service)

#set service type column
VMH_Raw_90[
  ,Service_Type := fcase(Transit_Day %in% as.IDate(holidays_saturday@Data)
                         , "Saturday"
                         , Transit_Day %in% as.IDate(holidays_sunday@Data)
                         , "Sunday"
                         , weekdays(Transit_Day) %in% c("Monday"
                                                        , "Tuesday"
                                                        , "Wednesday"
                                                        , "Thursday"
                                                        , "Friday"
                                                        )#end c
                         , "Weekday"
                         , weekdays(Transit_Day) == "Saturday"
                         , "Saturday"
                         ,weekdays(Transit_Day) == "Sunday"
                         , "Sunday"
                         )#end fcase 
]


# detour ------------------------------------------------------------------


VMH_Raw_90[
  ,AdHocTripNumber := str_c(
   Inbound_Outbound
   ,str_remove_all(Transit_Day,"-")
   ,Trip
   ,Vehicle_ID
  )
]

VMH_Raw_90[ , uniqueN(AdHocTripNumber)]
#different than sams but its fine maybe
#this is not fine lol

# end detour --------------------------------------------------------------

zero_b_a_vehicles <- VMH_Raw_90[
  
  #get zero board alights
  , .(Boards = sum(Boards)
     , Alights = sum(Alights)
     )
  , .(Transit_Day
     , Vehicle_ID
     )
][
  Boards == 0 | Alights == 0 
][
  order(Transit_Day)
]




# detour ------------------------------------------------------------------

VMH_Raw_90[Date == "2021-01-11" & Vehicle_ID == 1991]

VMH_Raw[
  , .N
  , .(GPSStatus,CommStatus)
]

Vehicle_Message_History_raw_sample %>%
    filter(Transit_Day == "2021-01-11"
           ,Vehicle_ID == 1991
           )
#fuck
#investigate commstatus
# "The communications status of this vehicle at this time.  Possible values are:
# 0 - Bad GPS
# 1 - Bad Comms
# 2 - Good Comms
# 3 - Inactive (Out of Service)"


#end detour

VMH_Raw_90_no_zero <- VMH_Raw_90[ 
  !zero_b_a_vehicles
  , on = c("Transit_Day", "Vehicle_ID")
]


# check here
# VMH_Raw_90_no_zero[
#   ,.(Boards = sum(Boards)
#      ,Alights = sum(Alights))
#   ,.(Transit_Day,Vehicle_ID)] %>%
#   View()


#get outliers
VMH_Raw_90_no_zero[
  , .(Boards = sum(Boards)
     ,Alights = sum(Alights))
  , .(Transit_Day,Vehicle_ID)
] %>% 
  ggplot(aes(x=Boards)) +
  geom_histogram(binwidth = 1) +
  stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)


  
#adjust this!!!!!!!!!!!!
obvious_outlier <- 500

outlier_apc_vehicles <- VMH_Raw_90_no_zero[
  ,.(Boards = sum(Boards)
     ,Alights = sum(Alights))
  ,.(Transit_Day,Vehicle_ID)
][
  Boards > obvious_outlier
]

#get three standard deviations from the mean
three_deeves <- VMH_Raw_90_no_zero[
  !outlier_apc_vehicles
  ,on = c("Transit_Day","Vehicle_ID")
][
  ,.(
    sum(Boards)
    ,sum(Alights)
  )
  ,.(Transit_Day,Vehicle_ID)
  
][
  ,sd(V1)*3
]+
  VMH_Raw_90_no_zero[
    !outlier_apc_vehicles
    ,on = c("Transit_Day","Vehicle_ID")
  ][
    ,.(
      sum(Boards)
      ,sum(Alights)
      )
    ,.(Transit_Day,Vehicle_ID)
    ][,mean(V1)]

three_sd_or_pct_vmh <- VMH_Raw_90_no_zero[
  #remove outlier apc vehicles
  !outlier_apc_vehicles
  ,on = c("Transit_Day","Vehicle_ID")
][
  ,.(
    Boardings = sum(Boards)
    ,Alightings = sum(Alights)
  )
  ,.(Transit_Day,Vehicle_ID)
][
  ,pct_diff := (Boardings - Alightings)/ ((Boardings + Alightings)/2)
][
  Boardings > three_deeves & pct_diff > 0.1 |
  Boardings > three_deeves & pct_diff < -0.1
]

VMH_Raw_90_no_zero[
  #remove outlier apc vehicles
  !outlier_apc_vehicles
  ,on = c("Transit_Day","Vehicle_ID")
  ][
    ,.(
      Boardings = sum(Boards)
      ,Alightings = sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ][
      ,pct_diff := (Boardings - Alightings)/ ((Boardings + Alightings)/2)
      ][]

VMH_Raw_90_no_zero[]

#get clean vmh
VMH_90_clean <- VMH_Raw_90[
  !zero_b_a_vehicles
  ,on = c("Transit_Day","Vehicle_ID")
  ][
    !three_sd_or_pct_vmh
    ,on = c("Transit_Day","Vehicle_ID")
    ]

#get invalid vmh
VMH_90_invalid <- fsetdiff(VMH_Raw_90,VMH_90_clean)


# expansion method --------------------------------------------------------
valid_dt <- VMH_90_clean[, trip_start_hour := hour(min(Time))
             , AdHocTripNumber
             ][
               , .(VBoard = sum(Boards)
                   , VTrip = uniqueN(AdHocTripNumber))
               , .(Inbound_Outbound
                   , trip_start_hour
                   , Service_Type
                   )
               ]

invalid_dt <- VMH_90_invalid[, trip_start_hour := hour(min(Time))
               , AdHocTripNumber
               ][
                 , .(IVBoard = sum(Boards)
                     , IVTrip = uniqueN(AdHocTripNumber))
                 , .(Inbound_Outbound
                     , trip_start_hour
                     , Service_Type
                 )
                 ]

month_results <- merge.data.table(valid_dt
                                  , invalid_dt
                                  , by = c("Inbound_Outbound","trip_start_hour","Service_Type")
                                  , all.x = TRUE
                                  )

month_results[,Boardings_Per_VTrip := VBoard/VTrip
              ][, `:=` (IVTrip = fifelse(is.na(IVTrip)
                                         , 0
                                         , IVTrip
                                         )
                        , IVBoard = fifelse(is.na(IVBoard)
                                            , 0
                                            , IVBoard
                                            )
                        )
              ][, Boardings_to_add := IVTrip * Boardings_Per_VTrip
                ][, `:=` (Expanded_Boardings = Boardings_to_add + VBoard
                          , Percent_IVTrip = IVTrip / (IVTrip + VTrip)
                          )
                  ]



month_results[,sum(VTrip)/(sum(VTrip)+sum(IVTrip))]

sum(month_results[,Expanded_Boardings])

VMH_90_clean[Route == 90
             , sum(Boards)
             ]

VMH_90_clean[Stop_Id == 0
             ,sum(Boards)
             ]

Service_Day_Set <- VMH_Raw_90[,.(Service_Days_in_Month = uniqueN(Transit_Day))
                              ,Service_Type
                              ]


month_service_summary <- Service_Day_Set[
  month_results[, .(Total_Boardings = sum(Expanded_Boardings))
                , Service_Type
                ]
  ,on = c("Service_Type")
  ][,`:=`(Total_Boardings = round(Total_Boardings)
          , Average_Daily_Boardings = round(Total_Boardings / Service_Days_in_Month
                                            ,0)
          )
    ][]

month_service_summary

month_total_boardings <- round(month_results[,sum(Expanded_Boardings)],0)

sample_n(VMH_90_clean,1000) %>%
  leaflet() %>%
  addCircles() %>%
  addTiles()

# find problem trips ------------------------------------------------------

# 
# 
# x <- copy(VMH_90_clean)
# 
# x[
#   order(seconds_since_midnight),trip_start_hour := first(Clock_Hour),AdHocTripNumber
# ][
#   order(AdHocTripNumber,seconds_since_midnight)
# ]
# 
# VMH_Raw[,.N,.(Inbound_Outbound,Route)][order(Inbound_Outbound,Route)]
# 
# VMH_90_clean[,.N,.(Inbound_Outbound,Route,Stop_Id)]
# VMH_90_clean[Inbound_Outbound==14 & Route==999 & Stop_Id==70016]
# 
# 
# 
# VMH_Raw_90[Vehicle_ID==1972 & Transit_Day=="2021-01-01"][,.N,AdHocTripNumber]
# 
# VMH_90_clean[Vehicle_ID==1972 & Transit_Day=="2021-01-01"][,.N,AdHocTripNumber]
# 
# 
# VMH_Raw_90[Route == 90 & Inbound_Outbound %in% c(14,9)] %>%
# leaflet() %>%
# addCircles() %>%
# addTiles()


# detect problem trips ----------------------------------------------------

#cases
#nb - next trip enters local, changeover not detected at 66th
#nb - next trip enters local, changeover detected, but included by bounding erro
#nb - next trip turns around, changeover not detected, direction changes
#nb - next trip turns around, changeover not detected, direction stuck

#start by adding adhoc trip to vmh_raw
VMH_Raw_test <- copy(VMH_Raw)

VMH_Raw_test[
  , DateTest := fifelse(data.table::as.ITime(Time) < Transit_Day_Cutoff
                        , 1
                        , 0
  ) #end fifelse()
  ][
    , Transit_Day := fifelse(DateTest == 1
                             ,data.table::as.IDate(Time)-1
                             ,data.table::as.IDate(Time)
    )
    ][
      ,seconds_since_midnight := fifelse(Transit_Day == as.IDate(Time)
                                         ,as.numeric(as.ITime(Time) - as.ITime(Transit_Day))
                                         ,as.numeric(as.ITime(Time))+24*60*60)
      ][
        ,Clock_Hour := data.table::hour(Time)
        ]

VMH_Raw_test[
  ,AdHocTripNumber := str_c(
    Inbound_Outbound
    ,str_remove_all(Transit_Day,"-")
    ,Trip
    ,Vehicle_ID
  )
]

VMH_Raw_test[,sum(Boards),Route]

VMH_Raw_test

# how many trips have very few gps reports
 
VMH_Raw_90[,.N,AdHocTripNumber][order(-N)] %>%
  ggplot(aes(x = N)) +
  geom_histogram(binwidth = 1)

#okay, so that's a lot of 1's
VMH_90_clean[,.(sum(Boards),.N),AdHocTripNumber][N < 10][,.N,V1]

#but they're mostly zero boarding trips

VMH_Raw_90[,.N,AdHocTripNumber][N < 50][]


VMH_Raw_test[AdHocTripNumber == "02020022601986"][order(data.table::as.ITime(Time))] #%>% View()

# so that trip has some direction change issues
# how do we find out if trips will ever have different directions
# count unique directions for each trip number
# count by trip and direction and look

VMH_Raw_90[,uniqueN(Inbound_Outbound),Trip][,.N,.(V1)]

# okay so a wholeeeee bunch of trips have multiple directions
# let's investigate them

# there aren't that many boardings within deadhead we can probably just cut to
# 90 and 901 and 902
VMH_Raw_90[,sum(Boards),Route]

# let's do that
VMH_Raw_90[Route %in% c(90,901,902),uniqueN(Inbound_Outbound),Trip][,.N,V1]

# o...kay that still doesn't help.
# so... perhaps we shall investigate a few trips and see whats happened

VMH_Raw_90[
  Route %in% c(90,901,902)
][

  VMH_Raw_90[
    Route %in% c(90,901,902)
    ,uniqueN(Inbound_Outbound)
    ,Trip
    ][
      V1 == 2
      ]
  
  ,on = "Trip"
][
  order(Vehicle_ID, Transit_Day,Time)
  ][
    Transit_Day < "2020-02-02"
    ][
      Trip==740
      ] %>% View()

# strange
# so this one was caused by another vehicle popping in somehow? what?
# look for 1899 and 1979

VMH_Raw_90[
  Route %in% c(90,901,902)
  ][
    
    VMH_Raw_90[
      Route %in% c(90,901,902)
      ,uniqueN(Inbound_Outbound)
      ,Trip
      ][
        V1 == 2
        ]
    
    ,on = "Trip"
    ][
      order(Vehicle_ID, Transit_Day,Time)
      ][
        Transit_Day < "2020-02-02"
        ][
          Vehicle_ID %in% c(1899, 1979)
          ] %>% View()

# okay so it looks like 1979 incorrectly got pegged as on 740
 
VMH_Raw_test[Transit_Day == "2020-02-01"][Vehicle_ID %in% c(1899,1979)][order(Vehicle_ID,Transit_Day,Time)] %>% View()

# okay no, that's incorrect, it was a 902 trip that entered route 90

# lets test a local vs extension n
# count n inside bounds and compare to n outside bounds


north_lat_boundary <- 39.877512
south_lat_boundary <- 39.709468
garage_boundary <- -86.173321


test_total_n_per_trip <- VMH_Raw_test[
  Transit_Day == "2020-02-01"
  ][Vehicle_ID %in% c(1899
                      , 1979
                      )
    ][
      ,.(n_per_trip = .N)
      ,.(Trip
         , Vehicle_ID
         , Transit_Day
         )]

test_n_inside_bounds <- VMH_Raw_test[
  Latitude %between% c(south_lat_boundary,north_lat_boundary) & Longitude > garage_boundary][
    Transit_Day == "2020-02-01"
    ][Vehicle_ID %in% c(1899
                        , 1979
                        )
      ][
        ,.(n_inside_boundaries = .N)
        ,.(Trip
           , Vehicle_ID
           , Transit_Day
           )
        ]

test_total_n_per_trip[
  test_n_inside_bounds
  , on = c("Trip","Vehicle_ID","Transit_Day")
  ][
    ,pct_inside := n_inside_boundaries / n_per_trip
  ][Trip != 0][order(pct_inside)]


#okay so it looks there's the ones that are under 50 N
#then there are the ones that are over 50, and that's kinda it
#let's check out this 25/99
VMH_Raw_test[Trip == 843 & Vehicle_ID == 1899 & Transit_Day == "2020-02-01"] %>%
  View()

#okay so 66.67% looks like it will work, let's try it on the whole month

total_n_per_trip <- VMH_Raw_test[
  ,.(n_per_trip = .N)
  ,.(Trip
     , Vehicle_ID
     , Transit_Day
     )
  ]

n_inside_bounds <- VMH_Raw_test[
  Latitude %between% c(south_lat_boundary,north_lat_boundary) &
    Longitude > garage_boundary
  ][
    ,.(n_inside_boundaries = .N)
    ,.(Trip
       , Vehicle_ID
       , Transit_Day
       )
    ]

#lets plot n_inside_boundaries to see what's up
total_n_per_trip[
  n_inside_bounds
  , on = c("Trip","Vehicle_ID","Transit_Day")
  ][
    ,pct_inside := n_inside_boundaries / n_per_trip
    ][Trip != 0
      ][n_inside_boundaries < 400] %>% 
  ggplot(aes(x = n_inside_boundaries)) +
  geom_histogram(aes(y = cumsum(..count..)))+
  stat_bin(aes(y = cumsum(..count..)),geom = "line",color = "green")
  
# okay so looking at numbers isn't going to work

total_n_per_trip[
  n_inside_bounds
  , on = c("Trip","Vehicle_ID","Transit_Day")
  ][
    ,pct_inside := n_inside_boundaries / n_per_trip
    ][Trip != 0
      ]

brks <- seq(0,10000,by = 10)

VMH_Raw_90[
  ,.(number_gps = .N)
  ,.(Trip,Transit_Day)
  ][
    ,bin := findInterval(number_gps
                         ,brks
                         )
    ][
      ,.(.N
         ,m = mean(number_gps)
         )
      ,by = bin
      ][,bin := bin * 10
        ][order(bin)]

# okay so it looks like we can just get rid of a ton of the low count ones
# lets go back to our four cases
# we need some bounding boxes i think
# okay so lets detect trips that cross going north
# we'll need trips that are near 66th st and increasing latitude

VMH_Raw_test[Stop_Name == "College&66thNB"][Latitude > north_lat_boundary] %>%
  leaflet() %>%
  addCircles() %>%
  addTiles()

#uhhhh why are there so many stops so far north lol
VMH_Raw_test[Stop_Name == "College&66thNB"][Latitude > north_lat_boundary]

#okay let's develop using this as the test case
test_case <- VMH_Raw_test[Transit_Day == "2020-02-11" & Vehicle_ID == "1993"
             ][order(Time)
               ]

test_case[Clock_Hour %between% c(10,10),View(.SD)]

cbind(total_n_per_trip[Transit_Day == "2020-02-11" & Vehicle_ID == "1993"][order(Trip)]
,n_inside_bounds[Transit_Day == "2020-02-11" & Vehicle_ID == "1993"][order(Trip)]
)

#cut by lat/long
test_case_90 <- test_case[Latitude %between% c(south_lat_boundary,north_lat_boundary) & Longitude > garage_boundary]

test_case_90[test_case_90[order(Time),.I[1],.(Trip,Transit_Day,Vehicle_ID)]$V1][Trip != 0] %>%
leaflet() %>%
addCircles(label = ~paste(Trip,Time)) %>%
addTiles()

# what the fuck
# Trip 2348 in fletcher place?
VMH_Raw_test[Transit_Day == as.IDate("2020-02-11") & Vehicle_ID == "1993"] %>% View()

pal <- colorNumeric(palette = "RdYlBu"
                    ,domain = VMH_Raw_test[
                      Transit_Day == "2020-02-11" & 
                        Vehicle_ID == "1993" & 
                        Trip == 2242
                      
                    ]$Time
                    )

VMH_Raw_test[Transit_Day == "2020-02-11" & 
               Vehicle_ID == "1993" & 
               Trip == 2242
             ] %>%
leaflet() %>%
addCircles(label = ~paste(Trip,Time,Route),color = ~pal(Time),radius = 25) %>%
addTiles()  

test_case_n_per_trip <- test_case[
  , .(n_total = .N)
  , .(Vehicle_ID, Transit_Day, Trip)
  ]

test_case_n_90_per_trip <- test_case_90[
  , .(n_90 = .N)
  , .(Vehicle_ID, Transit_Day, Trip)
  ]

pct_90 <- test_case_n_90_per_trip[test_case_n_per_trip
                                  , on = c("Vehicle_ID"
                                           , "Transit_Day"
                                           , "Trip"
                                           )
                                  ][
                                    , pct := n_90/n_total
                                    ]

pct_90[Trip != 0
       ][
         , fcase(n_90 < 30
                 ,)
         ]















#okay so it didn't detect trip changeover
#lets look at factsegment or factadherence and see if its in there
condw <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                        Database = "DW_IndyGo", Port = 1433)

DimDate_search <- tbl(condw, "DimDate") %>%
  filter(CalendarDate == "2020-02-11") %>%
  collect()

DimTrip <- tbl(condw,"DimTrip") %>%
  collect()

DimVehicle <- tbl(condw,"DimVehicle") %>%
  collect()

DimStop <- tbl(condw,"DimStop") %>%
  collect()

FactHeadwayAdherenceRaw <- tbl(condw, "FactHeadwayAdherence") %>%   #select(-CoordinateList,-TimepointGeom, everything())%>% # CoordinateList and TimepointGeom break collection.
  filter(DateKey == !!DimDate_search$DateKey) %>%
  collect()


joined <- FactSegmentAdherenceRaw %>%
  left_join(DimTrip,by = "TripKey") %>% 
  left_join(DimVehicle, by = "VehicleKey") %>%
  left_join(DimStop, by = c("DepartStopKey" = "StopKey")) %>%
  data.table()



joined[VehicleReportLabel == 1993 & TripReportLabel %like% 2348
       ][,.(StopReportLabel,ArriveTimeActual,DepartTimeActual)
         ][order(DepartTimeActual)]




# it is not. this is a problem lol

# okay back to red line stuff.

