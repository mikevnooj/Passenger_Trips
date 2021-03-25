con_rep <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                          Database = "TransitAuthority_IndyGo_Reporting", 
                          Port = 1433)

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
    where a.Route in (90,901,902,16,31)
    and a.Time > '20190901'
    and a.Time < DATEADD(day,1,'20200314')"
  )
  )
  ) %>%
  collect() %>%
  data.table()

VMH_Raw[
  #add clocktime
  , c("ClockTime", "Date") := list(stringr::str_sub(Time, 12, 19)
                                  , stringr::str_sub(Time, 1, 10)
                                  )
][
  
  , DateTest := ifelse(ClockTime < "03:30:00"
                       , 1
                       , 0
                       )
][, Transit_Day := ifelse(DateTest == 1
                        , lubridate::as_date(Date) - 1
                        , lubridate::as_date(Date))
][
  , Transit_Day := lubridate::as_date("1970-01-01") +
    lubridate::days(Transit_Day)
]

VMH_Raw[ Stop_Id == 52953, .N, .(Transit_Day, Route,Stop_Id)
][
order(Route, Stop_Id, Transit_Day)
] %>% head(50)

data.table(grp = rep(c("A", "B", "C", "A", "B")
                     , c(2, 2, 3, 1, 2)
                     )
           , value = 1:10)[
,rleid := rleid(grp)
][]

rleid(DT$grp)


#okay so we need rleid 