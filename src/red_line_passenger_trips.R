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

con_rep <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW\\REPSQLP01", 
                          Database = "TransitAuthority_IndyGo_Reporting", 
                          Port = 1433)

# Time --------------------------------------------------------------------

this_month_Avail <- lubridate::floor_date(Sys.Date(), unit = "month")

last_month_Avail  <- lubridate::floor_date(lubridate::floor_date(Sys.Date()
                                                                 ,unit = "month"
                                                                 ) - 1 #end floor_date
                                           ,unit = "month"
                                           )#end floor_date

month <- "2021-03-01"

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

#confirm dates
VMH_Raw_90[,.N,Transit_Day][order(Transit_Day)]
#error catch here as well

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


VMH_Raw_90[
  ,AdHocTripNumber := str_c(
   Inbound_Outbound
   ,str_remove_all(Transit_Day,"-")
   ,Trip
   ,Vehicle_ID
  )
]


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


VMH_Raw_90_no_zero <- VMH_Raw_90[ 
  !zero_b_a_vehicles
  , on = c("Transit_Day", "Vehicle_ID")
]


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
      ][] %>% View()

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
       ,paste0("data//processed//VMH_90_Valid_Invalid//"
               , str_sub(VMH_StartTime,0,6)
               , "_valid_trips.csv"
               )
       )

fwrite(invalid_dt_xuehao
       ,paste0("data//processed//VMH_90_Valid_Invalid//"
               , str_sub(VMH_StartTime,0,6)
               , "_invalid_trips.csv"
               )
       )


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

con_rep <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW\\REPSQLP01", 
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



trips_operated[,.N,`Internal trp number`]



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

fread("data//trips_operated_90.csv")

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

