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

month <- "2021-02-01"

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
Transit_Day_Cutoff <- as.ITime("03:30:00")

VMH_Raw[
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





#get 90 and remove garage boardings
VMH_Raw_90 <- VMH_Raw[Latitude > 39.709468 & Latitude < 39.877512
                      ][Longitude > -86.173321
                        ][Route == 90
                          ]

#do transit day


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

zero_b_a_vehicles <- VMH_Raw[, .(Boards = sum(Boards)
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
#
#
#
#investigate commstatus
# "The communications status of this vehicle at this time.  Possible values are:
# 0 - Bad GPS
# 1 - Bad Comms
# 2 - Good Comms
# 3 - Inactive (Out of Service)"


#end detour

# end detour --------------------------------------------------------------


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
obvious_outlier <- 1250

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
    ][
      , nstops := uniqueN(Stop_Id)
      , AdHocTripNumber
      ][nstops >= 28 | nstops == 21
        ]

VMH_Raw_90[
  , nstops := uniqueN(Stop_Id)
  , AdHocTripNumber
][
  ,.N
  ,nstops
  ]

VMH_Raw_90[
  , nstops := uniqueN(Stop_Id)
  , AdHocTripNumber
  ][
    ,uniqueN(AdHocTripNumber)
    ,nstops
    ][order(nstops)]



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

valid_dt_xuehao <- VMH_90_clean[,.(Boards = sum(Boards))
                                ,.(Transit_Day
                                   , AdHocTripNumber
                                   , Inbound_Outbound
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


invalid_dt_xuehao <- VMH_90_invalid[,.(Boards = sum(Boards))
                                    ,.(AdHocTripNumber
                                       , Transit_Day
                                       , Inbound_Outbound
                                       , trip_start_hour
                                       , Service_Type
                                       )
                                    ]




fwrite(valid_dt_xuehao
       ,paste0("data//"
               , str_sub(VMH_StartTime,0,6)
               , "_valid_trips.csv"
               )
       )

fwrite(invalid_dt_xuehao
       ,paste0("data//"
               , str_sub(VMH_StartTime,0,6)
               , "_invalid_trips.csv"
               )
       )



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
                    ][]


month_results[,.(hourvb = sum(VBoard)
                 ,hourvt = sum(VTrip)
                 ,hourivt = sum(IVTrip)
                 ,avgb = sum(VBoard)/sum(VTrip)
                 )
              ,.(trip_start_hour
                 ,Service_Type)
              ][order(-Service_Type,trip_start_hour)
                ][]

#this was .865041
month_results[,sum(VTrip)/(sum(VTrip)+sum(IVTrip))]

#this was 87252.61
sum(month_results[,Expanded_Boardings])

VMH_90_clean[Route == 90
             , sum(Boards)
             ]

#this was 74944
VMH_90_clean[Stop_Id == 0
             ,sum(Boards)
             ]

#this was 1309

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

VMH_90_clean[,.(stops = uniqueN(Stop_Id)
                ,N = .N)
             ,
             AdHocTripNumber
             ] %>% View()

VMH_90_clean[,uniqueN(Stop_Id)
             ,AdHocTripNumber
             ][,.N,V1]


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
# 
# 
# 
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


# combine all the csvs for stat support -----------------------------------

files <- paste0("data//"
                , list.files("data//"
                             , pattern = "trips.csv"
                             )
                )

read_plus <- function(file){
  fread(file) %>%
    mutate(valid_invalid = strsplit(file,"_")[[1]][2])
}


trips_data <- files %>%
  purrr::map_df(~read_plus(.))

fwrite(trips_data,"data//FY2020_trips_data.csv")
trips_data[,.N,.(Inbound_Outbound,valid_invalid)]
trips_data[valid_invalid == "invalid"]
trips_data[valid_invalid == "valid"]



trips_operated <- fread("data//90_Trips_Operated_2020.csv")

trips_operated[,Transit_Day := mdy(Date)]


operated <- trips_operated[`In-S` == "TRUE" & Cancelled == "FALSE" & Type != "Deadhead" & From != To][,.N,Transit_Day][order(Transit_Day)]

apc <- trips_data[Transit_Day < "2021-01-01",.N,Transit_Day][order(Transit_Day)]

merge.data.table(operated,apc,by = "Transit_Day",suffixes = c("operated","apc"))[,missingapc := Noperated - Napc][,sum(N)]

apc_month <- 1

cbind(trips_data[Transit_Day < "2021-01-01", .N, .(month(Transit_Day),Service_Type)][order(-Service_Type)]
,trips_operated[`In-S` == "TRUE" & 
                  Cancelled == "FALSE" & 
                  Type != "Deadhead" &
                  From != To][, .(operated = .N)
                              , .(month(Transit_Day)
                                  , SchType
                                  )
                              ][order(-SchType)]
)[,diff := operated - N][]

trips_operated[`In-S` == "TRUE" & 
                 Cancelled == "FALSE" & 
                 Type != "Deadhead" &
                 From != To
               ]


# unfuck this situation ---------------------------------------------------

con_rep <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW\\REPSQLP02", 
                          Database = "TransitAuthority_IndyGo_Reporting", 
                          Port = 1433)

tbl(con_rep
    ,sql("Select top 10 * from
    avl.vVehicle_History
    where Time > '20200101'
         and Time <= '20200102'"
         )
    )



trip_info_active <-tbl(con_rep
                       ,"vTrip_Info_Active"
                       ) %>%
  collect() %>%
  data.table() 

trip_info_deleted <- tbl(con_rep
                         ,"vTrip_Info_Deleted"
                         ) %>%
  collect() %>%
  data.table()

trip_info_prev <- tbl(con_rep
                      ,"vTrip_Info_Prev"
                      ) %>%
  collect() %>%
  data.table()

trip_info_working <- tbl(con_rep
                         ,"vTrip_Info_Working"
                         ) %>%
  collect() %>%
  data.table()

trip_info <- rbind(
  trip_info_prev
  ,trip_info_deleted
  ,trip_info_prev
  ,trip_info_working
)

trip_info <- tbl(con_rep
                 ,"Trip_Info_Edits"
                 ) %>%
  collect() %>%
  data.table()


trips_operated <- fread("data//90_Trips_Operated_Raw.csv")

trips_operated <- trips_operated[`In-S` == "TRUE" & Cancelled == "FALSE"]

trip_info[External_Identifier %like% trips_operated$`Internal trp number`]

trips_operated[,uniqueN(`Internal trp number`)]


trip_info[,External_Identifier]
trip_info[str_length(External_Identifier) > 7]

trips_operated[str_length(`Internal trp number`) > 7]

x <- tbl(con_rep
         ,"vSchedule_Data_Attributes_Active"
         ) %>%
  collect() %>%
  data.table()

x[Type == "trip_info",.N,Name
  ]


x <- tbl(con_dw
         ,"DimTrip"
         ) %>%
  collect() %>%
  data.table()

x[,.N,TripNumber][TripInternalNumber %in% trips_operated[,unique(`Internal trp number`)]]
trips_operated[,.N,`Internal trp number`]

# -------------------------------------------------------------------------
# 64029 is apc trips
# 65360 is what i originally sent xuehao

trips_operated[,.N,.(From,To)]
trips_operated[From != To,.N,.(From,To)]
#bad combos
#from 96C to COLL66
#from MAE to UOI
#from GP to MAE
#from GP to UOI
#from UOI to GP
#from MAE to GP
trips_operated[,From_To := paste0(From,To)]

bad_combos <- c("96CCOLL66"
                ,"MAEUOI"
                ,"GPMAE"
                ,"GPUOI"
                ,"UOIGP"
                ,"MAEGP"
                ,"COLL6696C")

trips_operated_90 <- trips_operated[From != To & !From_To %in% bad_combos]

trips_operated_90[,.N,From_To]

northbound <- c("M38COLL66"
                , "DTC-GCOLL66"
                , "UOICOLL66"
                , "GPCOLL66"
                , "UOIDTC-G"
                , "UOIM38"
                , "GPM38"
                , "GPDTC-G"
                , "UOI96C"
                , "MAECOLL66"
                , "DTC-GM38"
                , "DTC-G96C"
                , "M3896C")

trips_operated_90[!From_To %in% northbound][,.N,From_To]

#direction
trips_operated_90[,Inbound_Outbound := fifelse(From_To %in% northbound
                                               , 0
                                               , 1
                                               )
                  ]
#trip start hour
trips_operated_90[,time := NULL]
trips_operated_90[,aorp := NULL]
trips_operated_90[,Transit_Day := NULL]
trips_operated_90[,DateTest := NULL]
trips_operated_90[,service_type := NULL]
trips_operated_90[,trip_start_hour := NULL]
trips_operated_90[,trip_start_test := NULL]
trips_operated_90[,trip_start_hour_test := NULL]

#okay we need to fix this ap thing god dammit
#x is midnight hour

trips_operated_90[, c("time","aorp") := tstrsplit(trips_operated_90$Start,"[a,p,x,;]")]

trips_operated_90[,aorp := fifelse(Start %like% "p"
                                   ,"p"
                                   ,"a"
                                   )
                  ][, time := as.integer(time)
                    ][,time := fifelse(Start %like% "p" & time < 1200 | Start %like% "x"
                                       , time + 1200
                                       , time
                                       , 0)
                      ][,time := fifelse(time > 2359
                                         ,time - 2400
                                         ,time)
                        ][, time := as.ITime(strptime(sprintf('%04d'
                                                              , time
                                                              )
                                                      , format='%H%M'
                                                      )
                                             )
                          ][, aorp := NULL
                            ]




#okay it is transit day compliant
trips_operated_90

trips_operated_90[
  ,service_type := fcase(as.IDate(mdy(Date)) %in% as.IDate(holidays_saturday@Data)
                         , "Saturday"
                         , as.IDate(mdy(Date)) %in% as.IDate(holidays_sunday@Data)
                         , "Sunday"
                         , weekdays(as.IDate(mdy(Date))) %in% c("Monday"
                                                                , "Tuesday"
                                                                , "Wednesday"
                                                                , "Thursday"
                                                                , "Friday"
                                                                )#end c
                         , "Weekday"
                         , weekdays(as.IDate(mdy(Date))) == "Saturday"
                         , "Saturday"
                         , weekdays(as.IDate(mdy(Date))) == "Sunday"
                         , "Sunday"
                         )#end fcase 
  ]


trips_operated_90[
  , AdHocTripNumber := str_c(
    Inbound_Outbound
    ,str_remove_all(as.IDate(mdy(Date)),"-")
    ,`Internal trp number`
    )
  ]

trips_operated_90[,trip_start_hour := data.table::hour(time)
                  ][,Transit_Day := as.IDate(mdy(Date))
                    ]

cbind(trips_operated_90[,.(operatedN=.N),.(trip_start_hour)][order(trip_start_hour)]
      , trips_data[trip_start_hour != 1,.N,.(trip_start_hour)][order(trip_start_hour)]
      )[,diff := operatedN - N][]



trips_operated_90[,trip_start_test := fifelse(From %in% c("96C","MAE","GP") & 
                                                data.table::minute(time) < 24 &
                                                time 
                                              , abs(time - 1*60*60)
                                              , time
                                              )
                  ][,trip_start_hour_test := data.table::hour(trip_start_test)][]

cbind(trips_operated_90[,.(operatedN=.N),trip_start_hour_test][order(trip_start_hour_test)]
      , trips_data[trip_start_hour != 1,.N,.(trip_start_hour)][order(trip_start_hour)]
      )[,diff := operatedN - N][]

merge.data.table(trips_data[,.N,.(Transit_Day,Service_Type,trip_start_hour)][order(Transit_Day,trip_start_hour)]
                 , trips_operated_90[,.N,.(Transit_Day,service_type,trip_start_hour_test)][order(Transit_Day,trip_start_hour_test)]
                 ,by.x = c("Transit_Day"
                           ,"trip_start_hour"
                           )
                 ,by.y = c("Transit_Day"
                           ,"trip_start_hour_test"
                           )
                 ,suffixes = c("_apc"
                               ,"_operated"
                               )
                 ,all = TRUE
                 )


# 1 is southbound
# 0 in northbound

#do transit_day
#

FY2020_90_trips_operated <- trips_operated_90[,.(AdHocTripNumber
                                                 , Transit_Day
                                                 , Inbound_Outbound
                                                 , trip_start_hour
                                                 , service_type)
                                              ][]

fwrite(FY2020_90_trips_operated,"data//FY2020_90_trips_operated.csv")

merge.data.table(trips_operated_90[trip_start_hour == 4
                                   , .(operatedN=.N)
                                   , .(trip_start_hour,Inbound_Outbound,Transit_Day)
                                   ][order(trip_start_hour,Inbound_Outbound)
                                     ]
                 , trips_data[Inbound_Outbound != 9 & Inbound_Outbound != 14 & trip_start_hour != 1 & valid_invalid == "valid"
                              ,.(apcN = .N)
                              , .(trip_start_hour,Inbound_Outbound,Transit_Day)
                              ][order(trip_start_hour,Inbound_Outbound)
                                ]
                 , by = c("trip_start_hour","Inbound_Outbound","Transit_Day")
                 , aall = TRUE
                 )[,diff := operatedN - apcN][]


trips_operated_90[trip_start_hour == 5][order(time)]


trips_data <- trips_data[Transit_Day < as.IDate("2021-01-01") & Inbound_Outbound != 14 & Inbound_Outbound != 9]

trips_data[,.N,trip_start_hour]

trips_data[,trip_start_hour := fifelse(trip_start_hour == 1
                                       ,0
                                       ,trip_start_hour)]



trips_operated_90[Transit_Day <= "2020-01-31"
                  ][, .N
                    , .(Inbound_Outbound
                        , service_type
                        , trip_start_hour
                        )
                    ]


trips_data[valid_invalid == "valid",sum(Boards),.(trip_start_hour,Service_Type,Inbound_Outbound)][order(-Service_Type,trip_start_hour,Inbound_Outbound)]



# okay so first we need averages by hour and type for the whole ye --------
# 
# 
trips_data_valid <- trips_data[!trips_data[Transit_Day <= "2020-01-31" & Boards > 757 | Transit_Day %between% c("2020-02-01","2020-02-29") & Boards > 1000],on = c("AdHocTripNumber")]


avg_boardings_by_stratum <- trips_data_valid[valid_invalid == "valid" #& Transit_Day <= as.IDate("2020-01-31")
                                       ,.(boards = sum(Boards)
                                          , trips = .N
                                          )
                                       ,.(trip_start_hour
                                          , Service_Type
                                          , Inbound_Outbound              
                                          )
                                       ][,avg_boardings := boards/trips
                                         ]

trips_operated_per_stratum_january <- FY2020_90_trips_operated[#Transit_Day <= as.IDate("2020-01-31")
                                                        , .(trips_operated = .N) 
                                                        , .(trip_start_hour
                                                            , service_type
                                                            , Inbound_Outbound
                                                            )
                                                        ][order(Inbound_Outbound, -service_type,trip_start_hour)] %>% View()

merge.data.table(trips_operated_per_stratum_january
                 , avg_boardings_by_stratum
                 , by.x = c("service_type"
                          , "trip_start_hour"
                          , "Inbound_Outbound"
                          )
                 ,by.y = c("Service_Type"
                           , "trip_start_hour"
                           , "Inbound_Outbound"
                           )
                 ,all = TRUE
                 )[,final_boardings := trips_operated * avg_boardings
                   ][order(Inbound_Outbound,-service_type,trip_start_hour)] %>% View()





[,sum(final_boardings)
                     ]

