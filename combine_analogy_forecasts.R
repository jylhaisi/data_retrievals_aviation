# This script combines previously calculated anbalogy forecasts from 11/16...08/17 to those of 09/17...12/17.
# And saves the combined files to a new directory.
require(MOSpointutils)

# station 11 unfortunately seems to be corrupted in the old data so do not use that!!!
analogy_forecast_stations <- c(8,16,20,19,26,24,22,6,7,18,1,4,13,2,5,29,3)
old_directory <- ("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/sorted/analogy_forecasts/1116_0817/")
new_directory <- ("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/sorted/analogy_forecasts/0917_1217/")
combined_directory <- ("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/sorted/analogy_forecasts/1116_1217/")

eval(parse(text=paste0("old_files <- list.files(\"",old_directory,"\")")))
old_files <- old_files[grep("analogies",old_files)]
eval(parse(text=paste0("new_files <- list.files(\"",new_directory,"\")")))
new_files <- new_files[grep("analogies",new_files)]
available_files <- intersect(old_files,new_files)
rm(old_files)
rm(new_files)

for (station_id_no in analogy_forecast_stations) {
  # read in old and new data
  used_data_file <- available_files[grep(paste0("_",station_id_no,"_"),available_files)]
  if (!length(used_data_file)==FALSE) {
    old_data <- loadRData(paste0(old_directory,used_data_file))
    new_data <- loadRData(paste0(new_directory,used_data_file))
    # combine these and remove duplicates
    combined_data <- rbind(old_data,new_data)
    if (sum(duplicated(combined_data))>0) {
      combined_data <- combined_data[-which(duplicated(combined_data)),]
    }
    # Save combined data into a file of its own
    save(file=paste0(combined_directory,"station_id_",station_id_no,"_analogies.RData"),list="combined_data")
    
    rm(old_data)
    rm(new_data)
    rm(combined_data)
  }
  
  rm(used_data_file)
}

# Checking how much missing values are there in analogy forecasts
for (station_id_no in c(16,18,19,20,22,24,26,29,2,3,4,5,6,7,8)) {
  jape <- loadRData(paste0("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/sorted/analogy_forecasts/1116_1217/station_id_",station_id_no,"_analogies.RData"))
  print(paste(station_id_no,(sum(is.na(jape$value)))))
  rm(jape)
}


