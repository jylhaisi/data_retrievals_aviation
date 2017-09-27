# This script loads allMETARvars from RData files and sorts the data into stationwise files according to timestamp/variable number.
rm(list=ls())

loadRData <- function(fileName){
  #loads an RData file into the function's own environment and returns it
  load(fileName)
  get(ls()[ls() != "fileName"])
}

# Välttämättömät asema- ja muuttujalistat yms.
source("../point_data_analysis/load_libraries_tables_and_open_connections.R")
source("hae_aviation_scripts.R")

# THIS COMMENTED PART ONLY NEEDS TO BE RUN ONLY ONCE AS IT SAVES THE RESULTS TO FILES! THIS TAKES AWAY ALL THE NOT-NEEDED DATA FROM THE MONTHLY DATA FILES
# data_directory <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/")
# data_directory_output <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/")
# # First altering result files so that they do not need to be individually altered in workspace for each station_id_no
# for (year in 2000:2017) {
#   for (month in 1:12) {
#     if (month<10) {
#       month1 <- paste("0",month,sep="")
#     } else {
#       month1 <- month
#     }
#     print(paste(year,month1))
#     ### Loading manual METAR data ###
#     data_file <- paste0(data_directory,"all_METARvarshavainnot_METAR_",year,"_",month1,"_8_allstations.RData")
#     month_data <- loadRData(data_file)
#     
#     # If data is not empty, remove all other than auto observations and observations that are not done on conventional observation times
#     if (do.call(dim,list(x=month_data))[1]!=0) {
#       # Only taking into account auto-METARS even if the data should not contain anything else (message_type_id==8)
#       month_data <- subset(month_data,message_type_id==8)
#       # Removing timestamps which are not either 20 past or 10 to (THERE IS QUITE A BIT OF THESE! THE SENSITIVITY OF THE RESULTS [ESPECIALLY DATA AVAILABILITY] TO THIS PHASE CAN BE SUBSTANTIAL!)
#       month_data <- month_data[which(format(month_data[["obstime"]],"%M") %in% c("20","50")),]
#     }
#     # Saving slightly altered data
#     save(file=paste0(data_directory_output,year,"_",month,".RData"),month_data)
#     rm(data_file)
#     rm(month_data)
#     rm(month1)
#   }
#   rm(month)
# }
# rm(year)
# rm(data_directory)
# rm(data_directory_output)




data_directory <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/")
data_directory_sorted <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/sorted/")

# Assigning result matrices (individually for each station, )
all_station_ids <- (unique(all_aviation_lists[["station"]]["id"]))
all_station_ids <- all_station_ids[order(all_station_ids$id),]
all_time_stamps <- seq(from = as.POSIXct("2000-01-01 00:20:00 GMT", tz = "GMT"), to = with_tz(round.POSIXt(Sys.time(), "hours"), tz = "GMT"), by = "30 mins")
all_metar_vars <- c(1,4,10,20,501,502,503,504,505,506,507,508,513,514,515,516,517,521,522,745,746,747,748,840)
assigned_data_frame <- data.frame(matrix(NA, ncol = length(all_metar_vars)+1, nrow = length(all_time_stamps)))
names(assigned_data_frame)[1] <- "obstime"
names(assigned_data_frame)[2:dim(assigned_data_frame)[2]] <- paste0(all_metar_vars)
assigned_data_frame[,1] <- all_time_stamps
rm(all_time_stamps)
# Going through one station at a time
for (station_id_no in all_station_ids) {
  print(station_id_no)
  assigned_station_id <- paste0("station_id_",station_id_no)
  assign(assigned_station_id, assigned_data_frame)
  # Data is retrieved in one month chunks, as otherwise the extent would be too much
  for (year in 2000:2017) {
    for (month in 1:12) {
      if (month<10) {
        month1 <- paste("0",month,sep="")
      } else {
        month1 <- month
      }
      # print(paste(year,month1))
      ### Loading manual METAR data ###
      data_file <- paste0(data_directory,year,"_",month,".RData")
      month_data <- loadRData(data_file)
      
      # If data is empty, remove loaded data vector and move onto next month
      if (do.call(dim,list(x=month_data))[1]==0) {
        rm(data_file)
        rm(month_data)
        next
      } else {
        # picking station-wise data separately and assigning data to variable rows/columns where it belongs to.
        station_id_data <- filter(month_data,station_id==station_id_no)
        for (variable in 1:length(all_metar_vars)) {
          station_id_variable_data <- filter(station_id_data,param_id==all_metar_vars[variable])
          # This is too complicated...
          # eval(subs(do.call(assign,x=as.name(assigned_station_id)[[(variable+1)]][match(station_id_variable_data[["obstime"]],as.name(assigned_station_id)[["obstime"]])], value=station_id_variable_data[["value"]])))
          com_string <- paste0(assigned_station_id,"[[(variable+1)]][match(station_id_variable_data[[\"obstime\"]],",assigned_station_id,"[[\"obstime\"]])] <- station_id_variable_data[[\"value\"]]")
          eval(parse(text=com_string))
          rm(com_string)
          rm(station_id_variable_data)
        }
        rm(variable)
        rm(station_id_data)
        # print(station_id_no)
        
        rm(data_file)
        rm(month_data)
      }
      rm(month1)
    }
    rm(month)
  }
  rm(year)
  
  save(file=paste0(data_directory_sorted,assigned_station_id,"_sorted.RData"),list=assigned_station_id)
  rm(assigned_station_id)
}
rm(station_id_no)