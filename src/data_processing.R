files <- paste0("data//processed//VMH_90_Valid_Invalid"
                , list.files("data//processed//VMH_90_Valid_Invalid"
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


# we're going to get zero boarding and alighting vehicle days for... a long time? lmao
# start with 2019 VMH

con_rep <- DBI::dbConnect(odbc::odbc()
                          , Driver = "SQL Server"
                          , Server = "REPSQLP01VW\\REPSQLP01"
                          , Database = "TransitAuthority_IndyGo_Reporting"
                          , Port = 1433
                          )

start_date_vmh <- "20190101"

end_date_vmh <- "20200101"

vmh_raw <- tbl(con_rep,dbplyr::in_schema("avl","vVehicle_History")) %>%
  filter(Time > start_date_vmh
         ,Time < end_date_vmh
         ) %>%
  collect() %>%
  data.table()
