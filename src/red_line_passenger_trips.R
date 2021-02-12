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

con_rep <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                          Database = "TransitAuthority_IndyGo_Reporting", 
                          Port = 1433)

# Time --------------------------------------------------------------------

this_month_Avail <- lubridate::floor_date(Sys.Date(), unit = "month")

last_month_Avail  <- lubridate::floor_date((lubridate::floor_date(Sys.Date(),
                                                                  unit = "month") - 1),
                                           unit = "month")

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
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Time > '",VMH_StartTime,"'
    and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
    and Vehicle_ID >= 1970
    and Vehicle_ID <= 1999
    
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
VMH_Raw_90 <- VMH_Raw[Latitude > 39.709468 & Latitude < 39.877512][Longitude > -86.173321]

#do transit day
VMH_Raw_90 <- VMH_Raw_90[, c("ClockTime","Date") := list(str_sub(Time, 12, 19),str_sub(Time, 1, 10))
        ][, DateTest := ifelse(ClockTime<"03:30:00",1,0)
          ][, Transit_Day := ifelse(DateTest ==1
                                    ,as_date(Date)-1
                                    ,as_date(Date))
            ][,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
              ][Transit_Day >= last_month_Avail & Transit_Day < this_month_Avail
                ][Vehicle_ID > 1950 & Vehicle_ID < 2000 | Vehicle_ID == 1899]

#add seconds since midnight for later

VMH_Raw_90[
  ,`:=` (
    seconds_since_midnight = difftime(as.IDate(Date)
                                      ,as.IDate(Transit_Day)
                                      ,units = "secs"
                                      ) + difftime(as.ITime(Time)
                                                   ,as.ITime(Date)
                                                   ,units = "secs"
                                                   )
  ) #end :=
][
  #add clock hour
  ,Clock_Hour := str_sub(ClockTime,1,2)
]
#consider adding error catching here for times

# detour ------------------------------------------------------------------

x<-VMH_Raw_90[Transit_Day != Date][1]

x
#looks good

# end detour --------------------------------------------------------------


#confirm dates
VMH_Raw_90[,.N,Transit_Day][order(Transit_Day)]
#error catch here as well

# date comparo ------------------------------------------------------------
left_join(by_day,by_day_sam) %>% mutate(diff = N-`n()`)

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
  ,Service_Type := fcase(
    Transit_Day %in% as_date(holidays_saturday@Data), "Saturday"
    ,Transit_Day %in% as_date(holidays_sunday@Data), "Sunday"
    ,weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday"), "Weekday"
    ,weekdays(Transit_Day) == "Saturday", "Saturday"
    ,weekdays(Transit_Day) == "Sunday", "Sunday"
  )#end fcase 
]

VMH_Raw_90[,.N,.(Service_Type,Transit_Day)]

# detour ------------------------------------------------------------------


VMH_Raw_90[
  ,AdHocTripNumber := str_c(
   Inbound_Outbound
   ,str_remove_all(Transit_Day,"-")
   ,Trip
   ,Vehicle_ID
  )
]

VMH_Raw_90[,uniqueN(AdHocTripNumber)]
#different than sams


# end detour --------------------------------------------------------------

zero_b_a_vehicles <- VMH_Raw_90[
  
  #get zero board alights
  ,.(Boards = sum(Boards)
     ,Alights = sum(Alights))
  ,.(Transit_Day,Vehicle_ID)
][
  Boards == 0 | Alights == 0 
][order(Transit_Day)]


# detour ------------------------------------------------------------------

zero_boarding_alighting_vehicles_VMH<-data.table(zero_boarding_alighting_vehicles_VMH)

merge.data.table(zero_boarding_alighting_vehicles_VMH,zero_b_a_vehicles,by = c("Transit_Day","Vehicle_ID"),all = T)

VMH_Raw_90[Date == "2021-01-11" & Vehicle_ID == 1991]

VMH_Raw[,.N,.(GPSStatus,CommStatus)]

Vehicle_Message_History_raw_sample %>%
    filter(Transit_Day == "2021-01-11"
         ,Vehicle_ID == 1991)
#fuck
#investigate commstatus
# "The communications status of this vehicle at this time.  Possible values are:
# 0 - Bad GPS
# 1 - Bad Comms
# 2 - Good Comms
# 3 - Inactive (Out of Service)"


#end detour

VMH_Raw_90_no_zero <- VMH_Raw_90[!zero_b_a_vehicles, on = c("Transit_Day","Vehicle_ID")]





# check here
# VMH_Raw_90_no_zero[
#   ,.(Boards = sum(Boards)
#      ,Alights = sum(Alights))
#   ,.(Transit_Day,Vehicle_ID)] %>%
#   View()

#get outliers
VMH_Raw_90_no_zero[
  ,.(Boards = sum(Boards)
     ,Alights = sum(Alights))
  ,.(Transit_Day,Vehicle_ID)
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
valid_dt <- 1

VMH_90_clean %>%
  group_by(AdHocTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(trip_start_hour = first(Clock_Hour)) %>%
  arrange(AdHocTripNumber,seconds_since_midnight) %>%
  group_by(Inbound_Outbound,trip_start_hour,Service_Type) %>%
  summarise(VBoard = sum(Boards)
            ,VTrip = n_distinct(AdHocTripNumber))


# find problem trips ------------------------------------------------------


  
x <- copy(VMH_90_clean)

x[
  order(seconds_since_midnight),trip_start_hour := first(Clock_Hour),AdHocTripNumber
][
  order(AdHocTripNumber,seconds_since_midnight)
]

VMH_Raw[,.N,.(Inbound_Outbound,Route)][order(Inbound_Outbound,Route)]

VMH_90_clean[,.N,.(Inbound_Outbound,Route,Stop_Id)]
VMH_90_clean[Inbound_Outbound==14 & Route==999 & Stop_Id==70016]



VMH_Raw_90[Vehicle_ID==1972 & Transit_Day=="2021-01-01"][,.N,AdHocTripNumber]

VMH_90_clean[Vehicle_ID==1972 & Transit_Day=="2021-01-01"][,.N,AdHocTripNumber]


VMH_Raw_90[Route == 902 | Route == 901,.N,Inbound_Outbound]

leaflet() %>%
addCircles() %>%
addTiles()

