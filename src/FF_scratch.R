# 6/26/20 - station activity for service planning
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

