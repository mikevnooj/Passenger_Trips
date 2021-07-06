library(data.table)
library(dplyr)

ips_shortlist <- data.table::fread("data//raw//IPS_Short_List.csv")

ips_shortlist[, school_name := fifelse(stu_sch_code == 716
                                       ,  "716 - ARSENAL TECH HIGH SCHOOL"
                                       , sch_name
                                       )
              ][
                , minutes_am_bell_diff := as.integer(substr(`AM Bell - Transit Arrival Difference`
                                                            ,1
                                                            ,2
                                                            ))
                                                     ]

ips_shortlist[,mean(as.integer(substr(`AM Bell - Transit Arrival Difference`
                                      ,
                                      1
                                      ,2
                                      )
                               )
                    )
              ]


ips_shortlist[,.N,`AM Bell - Transit Arrival Difference`]

22+18+18+15+11+24+4


ips_shortlist[,as.integer(substr(`AM Bell - Transit Arrival Difference`,1,2))]

school_am_stops <- ips_shortlist[, .(student_count = .N)
                                 , .(school_name
                                     ,route = am_routes_taken
                                     ,stop_name = am_school_stop
                                     ,am_arrival_time
                                     )
                                 ][order(stop_name
                                         ,-school_name
                                         )
                                   ]

school_pm_stops <- ips_shortlist[, .(student_count = .N)
                                 , .(school_name
                                     ,route = pm_routes_taken_Direct
                                     ,stop_name = pm_school_stop
                                     ,pm_departure_time
                                 )
                                 ][order(stop_name
                                         ,-school_name
                                 )
                                 ]

#fwrite(school_am_stops,"data//processed//IPS_AM_Stops.csv")

fwrite(school_pm_stops,"data//processed//IPS_PM_Stops.csv")

school_am_stops[,sum(student_count),route][order(route)]


library(DBI)
start_date <- "20200101"
end_date <- "20200131"

con_DW <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW", 
                         Database = "DW_IndyGo", Port = 1433)

DimStop <- dplyr::tbl(con_DW,"DimStop") %>%
  collect() %>%
  data.table()

DimStop_School <- DimStop[StopDesc %in% school_am_stops[,stop_name]]

#1003 is our key
DimServiceLevel <- tbl(con_DW,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")


DimDate_full <- tbl(con_DW, "DimDate") %>%
  collect() %>% 
  setDT(key = "DateKey")


# get farekey

DimFare <- tbl(con_DW, "DimFare") %>%
  collect() %>%
  setDT(key = "FareKey")

# get Routes
DimRoute <- tbl(con_DW,"DimRoute") %>%
  filter(RouteReportLabel %in% local(as.character(unique(school_am_stops$route)))) %>%
  collect() %>% 
  setDT(key = "RouteKey")

# get FF boarding and alighting

DimDate_start_pre_cov_ips <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "01/01/2020") %>% 
  collect() %>% 
  setDT(key = "DateKey")

DimDate_end_pre_cov_ips <- tbl(con_DW, "DimDate") %>%
  filter(CalendarDateChar == "01/31/2020") %>%
  collect() %>% 
  setDT(key = "DateKey")

  
FactFare <- tbl(con_DW, "FactFare") %>%
  filter(
    FareKey %in% c(1001,1002,1003)
    , DateKey >= local(DimDate_start_pre_cov_ips$DateKey)
    , DateKey <= local(DimDate_end_pre_cov_ips$DateKey)
    , StopKey %in% local(DimStop_School$StopID)
    , RouteKey %in% local(DimRoute$RouteKey)
  ) %>%
  collect() %>% 
  setDT()




# Make some Box and Whiskers ----------------------------------------------

quartiles <- fread("data//processed//IPS_Onboard_Quartiles.csv")

