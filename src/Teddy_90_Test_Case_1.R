library(data.table)
library(leaflet)
library(dplyr)

#need to have installed
#stringr
#lubridate
#ggplot2

# Database Connections ----------------------------------------------------

con_rep <- DBI::dbConnect(odbc::odbc()
                          , Driver = "SQL Server"
                          , Server = "REPSQLP01VW\\REPSQLP02"
                          , Database = "TransitAuthority_IndyGo_Reporting"
                          , Port = 1433
                          )

# Time --------------------------------------------------------------------

month <- "2021-04-01"

this_month_Avail <- lubridate::floor_date(x = lubridate::as_date(month)
                                          , unit = "month"
)

last_month_Avail <- lubridate::floor_date(lubridate::floor_date(lubridate::as_date(month)
                                                                , unit = "month"
                                                                ) - 1 #end lubridate::floor_date
                                          ,unit = "month"
                                          )#end lubridate::floor_date








VMH_StartTime <- stringr::str_remove_all(last_month_Avail
                                         , "-"
                                         )

VMH_EndTime <- stringr::str_remove_all(this_month_Avail
                                       , "-"
                                       )


#paste0 the query
VMH_Raw <- tbl(
  con_rep,sql(
    paste0(
      "select a.Time
    , a.Route
    , Boards
    , Alights
    , Trip
    , Vehicle_ID
    , Stop_Name
    , Stop_Id
    , Inbound_Outbound
    , Departure_Time
    , Latitude
    , Longitude
    , GPSStatus
    , CommStatus
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Route like '90%'
    and a.Time > '", VMH_StartTime, "'
    and a.Time < DATEADD(day,1,'", VMH_EndTime, "')
    
    UNION
    
    select a.Time
    , a.Route
    , Boards
    , Alights
    , Trip
    , Vehicle_ID
    , Stop_Name
    , Stop_Id
    , Inbound_Outbound
    , Departure_Time
    , Latitude
    , Longitude
    , GPSStatus
    , CommStatus
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Time > '", VMH_StartTime, "'
    and a.Time < DATEADD(day,1,'", VMH_EndTime, "')
    and Vehicle_ID >= 1970
    and Vehicle_ID <= 1999
    
    UNION
    
      select a.Time
      , a.Route
      , Boards
      , Alights
      , Trip
      , Vehicle_ID
      , Stop_Name
      , Stop_Id
      , Inbound_Outbound
      , Departure_Time
      , Latitude
      , Longitude
      , GPSStatus
      , CommStatus
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Time > '", VMH_StartTime, "'
      and a.Time < DATEADD(day,1,'", VMH_EndTime, "')
      and Vehicle_ID = 1899"
    )#end paste
  )#endsql
) %>% #end tbl
  collect() %>%
  data.table()


VMH_Raw_test <- copy(VMH_Raw)

Transit_Day_Cutoff <- as.ITime("03:30:00")

VMH_Raw_test[
  , DateTest := fifelse(data.table::as.ITime(Time) < Transit_Day_Cutoff
                        , 1
                        , 0
                        ) #end fifelse()
  ][
    , Transit_Day := fifelse(DateTest == 1
                             , data.table::as.IDate(Time)-1
                             , data.table::as.IDate(Time)
                             )
    ][
      ,seconds_since_midnight := fifelse(Transit_Day == as.IDate(Time)
                                         , as.numeric(as.ITime(Time) - as.ITime(Transit_Day)
                                                      )
                                         , as.numeric(as.ITime(Time))+ 24 * 60 * 60
                                         )
      ][
        ,Clock_Hour := data.table::hour(Time)
        ]




VMH_Raw_test[
  , AdHocTripNumber := stringr::str_c(Inbound_Outbound
                                     , stringr::str_remove_all(Transit_Day
                                                               , "-"
                                                               )
                                     , Trip
                                     , Vehicle_ID
                                     )
  ]

# how many trips have very few gps reports

VMH_Raw_test[
  , .N 
  , AdHocTripNumber
  ][order(-N)] %>%
  ggplot2::ggplot(ggplot2::aes(x = N)) +
  ggplot2::geom_histogram(binwidth = 1)

#okay, so that's a lot of 1's
VMH_Raw_test[
  , .(sum(Boards)
      , .N
      )
  , AdHocTripNumber
  ][N < 10
    ][
      , .N
      , V1
      ]
#and there are a ton that have zero boardings
#not great!



# lets test a local vs extension n
# count n inside bounds and compare to n outside bounds


north_lat_boundary <- 39.877512
south_lat_boundary <- 39.709468
garage_boundary <- -86.173321


test_n_per_trip <- VMH_Raw_test[
  Transit_Day == "2021-03-01"
  ][
    ,.(n_per_trip = .N)
    ,.(Trip
       , Vehicle_ID
       , Transit_Day
       )
    ]

test_n_inside_bounds <- VMH_Raw_test[
  Latitude %between% c(south_lat_boundary,north_lat_boundary) & Longitude > garage_boundary][
    Transit_Day == "2021-03-01"
    ][
      , .(n_inside_boundaries = .N)
      , .(Trip
         , Vehicle_ID
         , Transit_Day
         )
      ]


test_n_per_trip[
  test_n_inside_bounds
  , on = c("Trip"
           , "Vehicle_ID"
           , "Transit_Day"
           )
  ][
    , pct_inside := n_inside_boundaries / n_per_trip
    ][Trip != 0][order(pct_inside)]

#okay let's do it on the whole month actually

n_per_trip <- VMH_Raw_test[
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
#
n_per_trip[
  n_inside_bounds
  , on = c("Trip","Vehicle_ID","Transit_Day")
  ][
    ,pct_inside := n_inside_boundaries / n_per_trip
    ][Trip != 0
      ][n_inside_boundaries < 400] %>% 
  ggplot2::ggplot(ggplot2::aes(x = n_inside_boundaries)) +
  ggplot2::geom_histogram(binwidth = 1)

n_per_trip[
  n_inside_bounds
  , on = c("Trip","Vehicle_ID","Transit_Day")
  ][
    ,pct_inside := n_inside_boundaries / n_per_trip
    ][Trip != 0
      ][n_inside_boundaries < 400] %>% 
  ggplot2::ggplot(ggplot2::aes(x = n_inside_boundaries)) +
  ggplot2::geom_histogram(ggplot2::aes(y = cumsum(..count..)))+
  ggplot2::stat_bin(ggplot2::aes(y = cumsum(..count..)),geom = "line",color = "green")

# okay so looking at numbers isn't going to work
# but it looks like we can just get rid of a ton of the low count ones
# lets go back to our four cases
# we need some bounding boxes i think
# okay so lets detect trips that cross going north
# we'll need trips that are near 66th st and increasing latitude

VMH_Raw_test[Stop_Name == "College&66thNB"][Latitude > north_lat_boundary] %>%
  leaflet::leaflet() %>%
  leaflet::addCircles() %>%
  leaflet::addTiles()

# uhhhh why are there so many stops so far north lol
VMH_Raw_test[Stop_Name == "College&66thNB"][Latitude > north_lat_boundary]

# hahahah this is so bad

# okay let's develop using this as the test case
test_case <- VMH_Raw_test[Transit_Day == "2020-02-11" & Vehicle_ID == "1993"
                          ][order(Time)
                            ]
test_case[Clock_Hour %between% c(10,10),head(.SD,200)]

cbind(n_per_trip[Transit_Day == "2020-02-11" & Vehicle_ID == "1993"][order(Trip)]
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
VMH_Raw_test[Transit_Day == as.IDate("2020-02-11") & Vehicle_ID == "1993"]

# so we missed changeover, and then it somehow realized it was a new trip

# let's do the preceding trip by color

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
  , .(Vehicle_ID
      , Transit_Day
      , Trip
      )
  ]

test_case_n_90_per_trip <- test_case_90[
  , .(n_90 = .N)
  , .(Vehicle_ID
      , Transit_Day
      , Trip
      )
  ]

pct_90 <- test_case_n_90_per_trip[test_case_n_per_trip
                                  , on = c("Vehicle_ID"
                                           , "Transit_Day"
                                           , "Trip"
                                  )
                                  ][
                                    , pct := n_90/n_total
                                    ]

pct_90[Trip != 0]
