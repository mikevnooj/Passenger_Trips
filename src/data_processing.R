

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
