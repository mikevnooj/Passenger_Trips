# 11/6/19

# This script applies methods developed in the testing script.

# It does the following:

# - import Route 90 data
# - clean for Red Line
# - identify invalid trips
# - remove and then apply hour/service-type expansion factor to invalid trips.
# - get total boardings.

# Futher potential improvements include:

# - including onboard count on trips which *enter* Red Line section

### Revision history ###

# 11/07/19 - Added date cleaning (ie, just for october).

# 11/08/19 - Revised query to just include BYD vehicles.
#          - Developed, applied new methods.
#          - Began assessing ridership by station.

# 11/12/19 - Updated time queries
#          - Added alternative results, without counting pass-throughs
#          - Updated dates.

# 11/13/19 - Added boardings by station, on a few different metrics.

# 12/2/19 - Updated for November data.
#         - Added new vehicle (1899)
#         - Included pass-throughs for November numbers.
#         - Changed Date to Transit_Day in Service Type identification. 

# 12/11/19 - added platform analysis

# 1/6/20  - Migrated script for November data to this new script. Changed dates.
#           (consider improving the date function)
#         - Need to get Holiday data from TMDM!

# 1/7/20  - consider also using RL alternative results..

# 1/30/20 - improved date variables
#         - began adding back APC data for unreporting vehicle search.

# 2/4/20  - began comparing VMH and Apc_Data to identify data failures.
#           (There was data issue on 1/29, when Avail was down for many hours)

# 3/4/20  - reviewed script. edited for March.
#           Changed service day set calculation to raw as opposed to clean (should be better)

# 3/10/20 - issue: this script may not include 901 and 902 prior to switch... eh...
#           map it.

# 4/4/20  - NEED TO REMOVE GARAGE BOARDINGS! (done)

# 4/5/20  - Need to add daily figure. Need to get total number of trips from
#           alternative source. Should probably review adhoc trip procedure.
#           (Haven't done)

# 4/8/20  - Need to change service type..

# 5/8/20  - Updated for April. Closely reviewed results to ensure all validation
#           methods still required.

# 6/8/20 - Updated for May.

# 7/6/20 - Updated for June. Revised data pull with new method from testing script.

# 7/8/20 - Revised with new testing script. Script is now agnostic as to route;
#          just pulls BYD vehicles and applies geographic bounding box to segregate
#          route 90.
#        - Also removed Run_Id from adhoc trip number. This may have been causing issues
#          during trip change-over.

# 8/6/20 - ran for 7/20. May need to revist/revise/retire high_end_wacky_apc_vehicles_reiterated.

# 9/2/20 - ran for 8/20.

# Libraries

library(tidyverse)
library(leaflet)
library(magrittr)
library(timeDate)

### --- Database Connections --- ####

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

# ### --- Time --- ###

this_month_Avail <- lubridate::floor_date(Sys.Date()-365, unit = "month") #remove these when done

last_month_Avail  <- lubridate::floor_date((lubridate::floor_date(Sys.Date()-365, #remove these when done
                                                                  unit = "month") - 1),
                                           unit = "month")

this_month_Avail_GPS_search <- as.POSIXct(this_month_Avail) + 115200

last_month_Avail_GPS_search <- as.POSIXct(last_month_Avail) - 230400

### --- Data Import --- ###

Vehicle_Message_History_raw_sample <- tbl(dbplyr::src_dbi(con),
                                          dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > local(last_month_Avail_GPS_search), Time < local(this_month_Avail_GPS_search), # need to improve this...
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899) %>%
  dplyr::left_join(tbl(dbplyr::src_dbi(con),
                       dbplyr::in_schema("avl", "Vehicle_Avl_History")),
                   by = "Avl_History_Id") %>%
  collect()

### --- Data Cleaning --- ###

# Filter by latitude to create Red Line set

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Latitude > 39.709468, Latitude < 39.877512)

# Filter by longitude to remove garage boardings, if any

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Longitude > -86.173321)

# format dates in VMH

Vehicle_Message_History_raw_sample$Date <- as.Date(str_sub(Vehicle_Message_History_raw_sample$Time.x, 1, 10))

Vehicle_Message_History_raw_sample$Clock_Time <- str_sub(Vehicle_Message_History_raw_sample$Time.x, 12, 19)

Vehicle_Message_History_raw_sample$DateTest <- ifelse(Vehicle_Message_History_raw_sample$Clock_Time < 
                                                        "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_raw_sample$Transit_Day_Unix <- ifelse(Vehicle_Message_History_raw_sample$DateTest == 1,
                                                              lubridate::as_date(Vehicle_Message_History_raw_sample$Date - 1),
                                                              Vehicle_Message_History_raw_sample$Date)

# add two dates together

Vehicle_Message_History_raw_sample$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_raw_sample$Transit_Day <- Vehicle_Message_History_raw_sample$Epoch_Date + lubridate::days(Vehicle_Message_History_raw_sample$Transit_Day_Unix)


# just get Sept data

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Transit_Day >= last_month_Avail, Transit_Day < this_month_Avail)



# confirm dates

sort(unique(Vehicle_Message_History_raw_sample$Transit_Day))


# now set service types

# Vehicle_Message_History_raw_sample$Date <- as.Date(Vehicle_Message_History_raw_sample$CalendarDate, format='%m-%d-%Y')

Vehicle_Message_History_raw_sample$Year <- lubridate::year(Vehicle_Message_History_raw_sample$Transit_Day)
Vehicle_Message_History_raw_sample$Month <- lubridate::month(Vehicle_Message_History_raw_sample$Transit_Day)
Vehicle_Message_History_raw_sample$Day <- lubridate::wday(Vehicle_Message_History_raw_sample$Transit_Day, label=TRUE)

# set holiday variables

holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# set sunday/saturday holiday dates

holidays_sunday <- holiday(2000:2020, holidays_sunday_service ) 

holidays_saturday <- holiday(2000:2020, holidays_saturday_service ) 

# reformat holiday dates

holidays_sunday_formatted <- as.Date(holidays_sunday@Data)
holidays_saturday_formatted <- as.Date(holidays_saturday@Data)

# set weekday service

Vehicle_Message_History_raw_sample_weekdays <- Vehicle_Message_History_raw_sample %>%
  filter(Day != "Sat" , Day != "Sun",
         !Transit_Day %in% holidays_sunday_formatted,
         !Transit_Day %in% holidays_saturday_formatted)

# set saturday service

Vehicle_Message_History_raw_sample_saturday <- Vehicle_Message_History_raw_sample %>%
  filter(Day == "Sat" |
           Transit_Day %in% holidays_saturday_formatted)

# set Sunday service

Vehicle_Message_History_raw_sample_sunday <- Vehicle_Message_History_raw_sample %>%
  filter(Day == "Sun" |
           Transit_Day %in% holidays_sunday_formatted)

# mutate service type

Vehicle_Message_History_raw_sample_sunday$Service_Type <- "Sunday"
Vehicle_Message_History_raw_sample_saturday$Service_Type <- "Saturday"
Vehicle_Message_History_raw_sample_weekdays$Service_Type <- "Weekday"

# rebind

Vehicle_Message_History_raw_sample <- rbind(Vehicle_Message_History_raw_sample_weekdays, Vehicle_Message_History_raw_sample_sunday,
                                            Vehicle_Message_History_raw_sample_saturday)

# change service type for COVID weekdays -- come back to this

Vehicle_Message_History_raw_sample %>%
  group_by(Service_Type) %>%
  summarise(n =n())

# Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
#   mutate(Service_Type = ifelse(Transit_Day == c("2020-03-30"), "Saturday", 
#                         ifelse(Transit_Day == c("2020-03-31"), "Saturday", Service_Type)))

Vehicle_Message_History_raw_sample %>%
  group_by(Service_Type) %>%
  summarise(n =n())

# mutate calendar day for identifier:

Vehicle_Message_History_raw_sample$Transit_Day_for_mutate <- str_replace_all(Vehicle_Message_History_raw_sample$Transit_Day,"-", "")

# add unique trip identifier:

Vehicle_Message_History_raw_sample$AdHocUniqueTripNumber <- str_c(Vehicle_Message_History_raw_sample$Inbound_Outbound,
                                                                  Vehicle_Message_History_raw_sample$Transit_Day_for_mutate,
                                                                  Vehicle_Message_History_raw_sample$Trip,
                                                                  Vehicle_Message_History_raw_sample$Vehicle_ID)

### let's review trip procedure ###

sum(table(unique(Vehicle_Message_History_raw_sample$AdHocUniqueTripNumber))) # 6472

sum(table(unique(str_c(Vehicle_Message_History_raw_sample$Inbound_Outbound,
                       Vehicle_Message_History_raw_sample$Transit_Day_for_mutate,
                       Vehicle_Message_History_raw_sample$Trip,
                       Vehicle_Message_History_raw_sample$Vehicle_ID,
                       Vehicle_Message_History_raw_sample$Run_Id)))) # 6472

sum(table(unique(str_c(Vehicle_Message_History_raw_sample$Inbound_Outbound,
                       Vehicle_Message_History_raw_sample$Transit_Day_for_mutate,
                       Vehicle_Message_History_raw_sample$Trip,
                       Vehicle_Message_History_raw_sample$Vehicle_ID)))) # 6082 # so... trips with different run_ids?

sum(table(unique(str_c(
  Vehicle_Message_History_raw_sample$Transit_Day_for_mutate,
  Vehicle_Message_History_raw_sample$Trip,
  Vehicle_Message_History_raw_sample$Vehicle_ID,
  Vehicle_Message_History_raw_sample$Run_Id)))) # 6472 # No... there are trips with different directions..

sum(table(unique(str_c(
  Vehicle_Message_History_raw_sample$Transit_Day_for_mutate,
  Vehicle_Message_History_raw_sample$Trip,
  Vehicle_Message_History_raw_sample$Vehicle_ID)))) # 6082

# thesse mayyyy be including deadhead??
# OR! maybe there is a "bad" trip number? like deadheading?

sort(unique(Vehicle_Message_History_raw_sample$Trip))

# OR! there is trip changeover...

Vehicle_Message_History_raw_sample %>%
  group_by(Trip, In) # incomplete NOTE: we should probably develop a schedule set from something... maybe factsegmentschedule???

# let's look at hose trips that have different directions... (would be good if we compared to DW dataset...)

# now get clock hour

Vehicle_Message_History_raw_sample$Clock_Hour <- str_sub(Vehicle_Message_History_raw_sample$Clock_Time, 1, 2)

# get seconds since midnight

Vehicle_Message_History_raw_sample$seconds_between_dates <- difftime(Vehicle_Message_History_raw_sample$Date,
                                                                     Vehicle_Message_History_raw_sample$Transit_Day,
                                                                     units = "secs")

# maybe just need to use GPS_Time format...

Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_raw_sample$Time.x,
                                                                               Vehicle_Message_History_raw_sample$Date, 
                                                                               units = "secs")

# add those two seconds to get seconds since (true) midnight

Vehicle_Message_History_raw_sample$seconds_since_midnight <- Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time +
  Vehicle_Message_History_raw_sample$seconds_between_dates

# clean the format up (dplyr no like POSIXlt)

Vehicle_Message_History_raw_sample$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_raw_sample$seconds_since_midnight))


Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day) %>%
  summarise(n()) %>% view()

# now.... need to apply validation criteria to Vehicle_Message_History_raw_sample
# to develop set of valid/invalid trips...

# get days in which vehicles had no APC data

zero_boarding_alighting_vehicles_VMH  <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  filter(Boardings == 0 | Alightings == 0)

zero_boarding_alighting_vehicles_VMH 

# remove those vehicles on those days

# Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles, by = c("Transit_Day", "Vehicle_ID"))# %>%
# # filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

# Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
#   anti_join(zero_boarding_alighting_vehicles_APC, by = c("Transit_Day", "Vehicle_ID" = "MDCID"))# %>%
# filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

Vehicle_Message_History_raw_sample_no_zero_boardings <- Vehicle_Message_History_raw_sample %>%
  anti_join(zero_boarding_alighting_vehicles_VMH, by = c("Transit_Day", "Vehicle_ID"))# %>%
# filter(Vehicle_ID != bad_vehicle_set) # COME BACK TO THIS

# looks good! 
# now we need to identify vehicles with wacky APCs.

# Note... Red for Ed event on Nov. 19th may have seen true, high ridership. 
# Will need to examine daily boardings.

Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings = sum(Alights)) %>%
  arrange(Transit_Day) %>%
  View() # looks okay to me... (1994 still looks dicey)

# make variable that is two standard deviations from the mean

boardings_three_sd_from_mean <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
                                   group_by(Transit_Day, Vehicle_ID) %>%
                                   summarise(Boardings = sum(Boards),
                                             Alightings =sum(Alights)) %$%
                                   sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %$%
     mean(Boardings))

# get vehicles that are three sd from mean

wacky_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_three_sd_from_mean)

# get summary of wacky vehicles

wacky_apc_vehicles %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boardings))

# build obvious outlier set (for Option 4 - step 1)

Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 500 #this still holds for November...

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

# and then get vehicle days above 2 SDs above mean (for Option 4 - step 2)

boardings_three_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
                                                       group_by(Transit_Day, Vehicle_ID) %>%
                                                       summarise(Boardings = sum(Boards),
                                                                 Alightings = sum(Alights)) %>%
                                                       anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                       sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > boardings_three_sd_from_mean_outlier_set_removed)

# option 5 - (permutation of option 4) - remove outliers and then vehicles whose boardings/alightings difference are >5%, for vehicles above 2 sds above mean

Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  ggplot(aes(x = Boardings))+
  geom_histogram(binwidth = 1)+
  stat_bin(binwidth=1, geom="text", aes(label=..x..), vjust=-1.5) #1250 looks to be cut-off

obvious_outlier <- 500

outlier_apc_vehicles <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  filter(Boardings > obvious_outlier)

boardings_three_sd_from_mean_outlier_set_removed <- (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
                                                       group_by(Transit_Day, Vehicle_ID) %>%
                                                       summarise(Boardings = sum(Boards),
                                                                 Alightings = sum(Alights)) %>%
                                                       anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
                                                       sd(Boardings)*3) + 
  (Vehicle_Message_History_raw_sample_no_zero_boardings %>%
     group_by(Transit_Day, Vehicle_ID) %>%
     summarise(Boardings = sum(Boards),
               Alightings =sum(Alights)) %>%
     anti_join(outlier_apc_vehicles, by = c("Transit_Day", "Vehicle_ID")) %$%
     mean(Boardings))

high_end_wacky_apc_vehicles_reiterated <- Vehicle_Message_History_raw_sample %>%
  group_by(Transit_Day, Vehicle_ID) %>%
  summarise(Boardings = sum(Boards),
            Alightings =sum(Alights)) %>%
  mutate(Percent_Difference = (Boardings - Alightings)/ ((Boardings + Alightings)/2))%>%
  filter(Boardings > obvious_outlier |
           (Boardings > boardings_three_sd_from_mean_outlier_set_removed) &
           Percent_Difference > 0.1 | 
           (Boardings > boardings_three_sd_from_mean_outlier_set_removed) &
           Percent_Difference < -0.1)

# do final comparison

high_end_wacky_apc_vehicles_reiterated

# Vehicle_Message_History_raw_sample_no_zero_boardings %>% 
#   group_by(Transit_Day, Vehicle_ID) %>%
#   summarise(Boardings = sum(Boards),
#             Alightings = sum(Alights)) %>%
#   arrange(Transit_Day) %>%
#   View() # so.... a couple of 1994 vehicles sneak through but....

# remove those vehicles -- Option 5 -- remove obvious outliers, then 
# remove vehicles surpassing 5% threshold to vehicle-days w/ high boardings

Vehicle_Message_History_raw_sample_clean <- Vehicle_Message_History_raw_sample %>%
  anti_join(zero_boarding_alighting_vehicles_VMH, by = c("Transit_Day", "Vehicle_ID")) %>%
  anti_join(high_end_wacky_apc_vehicles_reiterated , by = c("Transit_Day", "Vehicle_ID"))

# # now build dataset of invalid data -- Option 3 (see above) AND Option 4 (see above) AND Option 5
# 
Vehicle_Message_History_raw_sample_invalid <- Vehicle_Message_History_raw_sample %>%
  anti_join(Vehicle_Message_History_raw_sample_clean)


# now let's do the expansion method...

valid_df <- Vehicle_Message_History_raw_sample_clean %>% # valid data
  group_by(AdHocUniqueTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
  arrange(AdHocUniqueTripNumber, seconds_since_midnight) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
  summarise(Valid_Boardings = sum(Boards),
            Valid_Trips = n_distinct(AdHocUniqueTripNumber)) 

# valid_pass_throughs <- Vehicle_Message_History_raw_sample_clean %>% # valid data for pass-throughs
#   group_by(AdHocUniqueTripNumber) %>%
#   arrange(seconds_since_midnight) %>%
#   mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
#   mutate(First_Onboard_Count = first(Onboard),
#          First_Boarding = first(Boards),
#          First_Alighting = first(Alights),
#          Starting_Pass_Through = first(Onboard) - first(Boards) + first(Alights)) %>%
#   group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type, AdHocUniqueTripNumber) %>%
#   summarise(Starting_Pass_Through_grouped = first(Starting_Pass_Through)) %>%
#   group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
#   summarise(Starting_Pass_Through_to_add = sum(Starting_Pass_Through_grouped))

invalid_df <- Vehicle_Message_History_raw_sample_invalid  %>% # invalid data
  group_by(AdHocUniqueTripNumber) %>%
  arrange(seconds_since_midnight) %>%
  mutate(Trip_Start_Hour = first(Clock_Hour)) %>%
  arrange(AdHocUniqueTripNumber, seconds_since_midnight) %>%
  group_by(Inbound_Outbound, Trip_Start_Hour, Service_Type) %>%
  summarise(Invalid_Boardings = sum(Boards),
            Invalid_Trips = n_distinct(AdHocUniqueTripNumber))

invalid_df %>% view()

August_results <- left_join(valid_df, invalid_df,
                           by = c("Inbound_Outbound", "Trip_Start_Hour", "Service_Type")) %>%
  mutate(Boardings_per_valid_trip = (Valid_Boardings) / Valid_Trips) %>%
  mutate(Boardings_to_add = ifelse(!is.na(Invalid_Trips),
                                   Invalid_Trips * Boardings_per_valid_trip,
                                   0)) %>%
  mutate(Expanded_boardings = Boardings_to_add + Valid_Boardings)  %>%
  mutate(Percent_Invalid_Trips  = Invalid_Trips / (Invalid_Trips + Valid_Trips))

sum(invalid_df$Invalid_Trips)
valid_df %>% view()
sum(valid_df$Valid_Trips)

# to get RL ridership: 

sum(August_results$Expanded_boardings, na.rm = TRUE)

sum(August_results$Valid_Trips, na.rm = TRUE)
sum(August_results$Invalid_Trips, na.rm = TRUE)

sum(August_results$Valid_Trips, na.rm = TRUE)/ (sum(August_results$Valid_Trips, na.rm = TRUE) + sum(August_results$Invalid_Trips, na.rm = TRUE))

# what is raw sum?

sum(Vehicle_Message_History_raw_sample_clean$Boards)

# what is raw sum from unattributed stops?

Vehicle_Message_History_raw_sample_clean %>%
  filter(Stop_Id == 0) %$%
  sum(Boards) # 

##### boardings by service type ####

Service_Day_Set <- Vehicle_Message_History_raw_sample  %>%
  group_by(Service_Type) %>%
  summarise(Service_Days_in_Month = n_distinct(Transit_Day)) # 

# research this issue:

Vehicle_Message_History_raw_sample_clean  %>%
  group_by(Service_Type) %>%
  summarise(Service_Days_in_Month = toString(unique(Transit_Day))) %>%
  View() 
# Now... how to join....

August_results %>%
  group_by(Service_Type) %>%
  summarise(Total_Boardings = sum(Expanded_boardings)) %>%
  left_join(Service_Day_Set, by = "Service_Type") %>%
  mutate(Average_Daily_Ridership = Total_Boardings / Service_Days_in_Month) %>%
  mutate_if(is.numeric, round, 0) %>%
  formattable::formattable()

August_service_summary <- August_results %>%
  group_by(Service_Type) %>%
  summarise(Total_Boardings = sum(Expanded_boardings)) %>%
  left_join(Service_Day_Set, by = "Service_Type") %>%
  mutate(Average_Daily_Ridership = Total_Boardings / Service_Days_in_Month) %>%
  mutate_if(is.numeric, round, 0) %>%
  gt::gt()

August_total_ridership <- round(sum(August_results$Expanded_boardings, na.rm = TRUE), 0)

August_service_summary

August_total_ridership



## extra

# map it. (a sample)

library(leaflet)

map <- leaflet(sample_n(Vehicle_Message_History_raw_sample_clean, 1000)) %>%
  addProviderTiles(providers$Stamen.Toner) %>%
  addCircles(color = "orange", weight = 3, 
             opacity = .1, fillOpacity = 0.5,
             highlightOptions = highlightOptions(color = "white", weight = 2,
                                                 bringToFront = TRUE),
             lat = ~Latitude,
             lng = ~Longitude)

map # looks OK!
