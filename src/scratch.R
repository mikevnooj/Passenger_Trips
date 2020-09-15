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
