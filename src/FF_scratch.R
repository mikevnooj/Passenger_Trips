# 6/26/20 - Thomas Coon wants to know average daily ridership and ADR by hour for each of
#           the following stops: 52263; 52264; 52265; 52266; 52302; 52303; 52304
library(data.table)
library(dplyr)
library(ggplot2)


# db connection

con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                       Database = "DW_IndyGo", Port = 1433)
 

# get applicable dates

DimServiceLevel <- tbl(con_DW,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")

DimDate <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "02/01/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_full <- tbl(con_DW, "DimDate") %>%
  collect() %>% 
  setDT(key = "DateKey")

# get stops
DimStop <- tbl(con_DW,"DimStop") %>% 
  collect() %>% 
  setDT(key = "StopKey")

# get timekey

DimTime <- tbl(con_DW, "DimTime") %>% 
  collect() %>% 
  setDT(key = "TimeKey")

# get farekey

DimFare <- tbl(con_DW, "DimFare") %>%
  collect() %>%
  setDT(key = "FareKey")

# get Routes
DimRoute <- tbl(con_DW,"DimRoute") %>%
  filter(RouteReportLabel %like% "90") %>%
  collect() %>% 
  setDT(key = "RouteKey")

# get FF boarding and alighting

FactFare <- tbl(con_DW, "FactFare") %>%
  filter(
    FareKey %in% c(1001, 1002),
    DateKey >= local(DimDate$DateKey),
    RouteKey %in% local(DimRoute$RouteKey)
  ) %>% #end filter
  collect() %>% 
  setDT()

### transformations ###

FF_joined <- DimDate[
  DimTime[
    DimFare[
      DimRoute[
        FactFare
        , on = "RouteKey"
        ]
      , on = "FareKey"
      ]
    , on = "TimeKey"
    ]
  , on = "DateKey"
] 

#get daily by vehicle
FF_joined_daily <- FF_joined[
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
FF_no_zero <- FF_joined[!FF_joined_zero,on = c(DateKey = "DateKey",VehicleKey = "VehicleKey")]

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



FF_summary <- 1
#get by day, service type, month
DimServiceLevel[
  DimDate_full[
    ,.(DateKey,CalendarDate)
  ][
    FF_clean[
      ,sum(FareCount)
      ,.(FareReportLabel,DateKey,Time24Hour,ServiceLevelKey)
    ][
      ,dcast(
        .SD
        ,DateKey + Time24Hour + ServiceLevelKey ~ FareReportLabel
        ,fill = 0
      )
      ]
      ,on = "DateKey"
      ]
  , on = "ServiceLevelKey"
]

[
  order(month(CalendarDate))
  ,.(Average_Boardings = round(mean(Boarding)))
  ,.(
    month = month(CalendarDate)
    ,hour = Time24Hour
    ,Service_Type = sub(".*-","",ServiceLevelReportLabel)
  )
][
  ,dcast(
    .SD
    ,hour + month ~ Service_Type
    ,fill = 0
  )
][order(month,hour)
][month <= 9
][
  ,`:=`(month = month.abb[month]
     ,hour = paste0(hour,":00"))
]


VMH_summary <- fread("data//Aletra_Hourly_Summary.csv")

FF_summary %>% str()

VMH_summary %>% str()

merge.data.table(FF_summary,VMH_summary,by = c("month","hour"),all = T)[
  ,`:=`(
    Sat_diff = Saturday.x-Saturday.y
    ,Sun_diff = Sunday.x-Sunday.y
    ,Wk_diff = Weekday.x-Weekday.y
  )
]




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
