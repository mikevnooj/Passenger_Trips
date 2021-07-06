# 2021/02/23 MPO ----------------------------------------------------------

library(dplyr)
library(data.table)
library(ggplot2)


# Import ------------------------------------------------------------------


#add Stop Ridership Col Names
Stop_Ridership_col_names <- c("Stop Number", "Stop Name", "Total Boardings", "Average Daily Boardings","Total Alightings","Average Daily Alightings","Days")


# Import ------------------------------------------------------------------


# db connectino

con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", Port = 1433)


# get applicable dates

DimServiceLevel <- tbl(con_DW,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")

DimDate_start_MPO_full_pre <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "10/01/2019") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_end_MPO_full_pre <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "11/01/2019") %>%
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
  collect() %>% 
  setDT(key = "RouteKey")

# get FF boarding and alighting

FactFare_MPO_pre <- tbl(con_DW, "FactFare") %>%
  filter(
    FareKey %in% c(1001, 1002)
    ,DateKey >= local(DimDate_start_MPO_full_pre$DateKey)
    ,DateKey <= local(DimDate_end_MPO_full_pre$DateKey)
  ) %>% #end filter
  collect() %>% 
  setDT()


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


calendar <- seq.Date(as.IDate(DimDate_start_MPO_full_pre[,CalendarDate])
                     ,as.IDate(DimDate_end_MPO_full_pre[,CalendarDate])
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
                   , DateTest := ifelse(ClockTime<as.ITime("03:30:00"),1,0)
                   ][
                     , Transit_Day := fifelse(
                       DateTest ==1
                       ,lubridate::as_date(Date)-1
                       ,lubridate::as_date(Date)
                     )#end fifelse
                     ][
                       ,Transit_Day := lubridate::as_date("1970-01-01")+lubridate::days(Transit_Day)
                       ]

FF_MPO_wkdy <- FactFare_MPO_pre[Service_Type == "Weekday"
                                ][Transit_Day >= DimDate_start_MPO_full_pre$CalendarDate 
                                  & Transit_Day < DimDate_end_MPO_full_pre$CalendarDate]

FF_MPO_wkdy[,.N,Transit_Day]

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



FactFareDateCount <- 
  FactFare_MPO_pre[
    Service_Type == "Weekday"
    ][
      ,.(Days = uniqueN(Transit_Day))
      ,StopID
      ]

FactFareDateCount[order(-Days)]

FactFare_MPO_pre[StopID == 10920, unique(Transit_Day),Service_Type]

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

fwrite(FF_MPO_pre_summary
       ,"data//output//201910_MPO_Stop.csv"
       )
FF_MPO_pre_summary[order(-`Average Daily Boardings`)] %>% View()
