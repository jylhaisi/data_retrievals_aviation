# This script loads allMETARvars from RData files and sorts the data into stationwise files according to timestamp/variable number.
rm(list=ls())
library(foreach)
library(doParallel)
registerDoParallel()

# A few functions that are needed
# This function loads RData and assigns it to the variable defined by user
loadRData <- function(fileName){
  #loads an RData file into the function's own environment and returns it
  load(fileName)
  get(ls()[ls() != "fileName"])
}
# This function calculates the smallest difference between two circularly defined values. Examples from circular variables are (azimuth angle, hour of the day, day of the year)
circular_variable_difference <- function(direction1, direction2, digit_base) {
  difference <- direction1 - direction2
  difference[difference > (digit_base/2)] <- difference[difference > (digit_base/2)] - digit_base
  difference[difference < (-(digit_base/2))] <- digit_base + difference[difference < (-(digit_base/2))]
  invisible(difference)
}

# Necessary station and variable lists etc.
source("../point_data_analysis/load_libraries_tables_and_open_connections.R")
source("hae_aviation_scripts.R")
data_directory <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/sorted/")
data_directory_output <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/all_METARvars/only_20_50_observations/sorted/analogy_forecasts/")
ifelse(!dir.exists(file.path(data_directory_output)), dir.create(file.path(data_directory_output)), FALSE)

all_station_ids <- (unique(all_aviation_lists[["station"]]["id"]))
all_station_ids <- all_station_ids[order(all_station_ids$id),]
# Defining additional variables 888 and 889. These mark the hour of the day (used in defining difference in hours) and day of the year (used in defining difference in days)
all_metar_vars <- c(1,4,10,20,501,502,503,504,505,506,507,508,513,514,515,516,517,521,522,745,746,747,748,840,888,889)
# begin_date=as.POSIXct("2017-07-01 00:20:00 GMT",tz="GMT")
# end_date=as.POSIXct("2017-07-25 06:20:00 GMT",tz="GMT")
begin_date=as.POSIXct("2016-11-01 00:20:00 GMT",tz="GMT")
end_date=as.POSIXct("2017-09-01 00:20:00 GMT",tz="GMT")
# end_date=as.POSIXct(Sys.time(),tz="GMT")
all_forecast_times <- seq(from=begin_date, to=end_date, by="3 hour")



# BELOW ARE LISTED ALL THE CRUCIAL ASSUMPTIONS WHEN FORMING THE ANALOGY FORECASTS. HOW SENSITIVE ARE THE RESULTS TO THESE ASSUMPTIONS???
### BEGIN
# FUZZY SETS FOR INDIVIDUAL VARIABLES IN METARS. THESE COULD BE MAYBE BETTER DEFINED USING SAMPLE CLIMATOLOGY INSTEAD OF THESE AD HOC DEFINITIONS!
similarity_values <- c(0,0.2,0.5,0.7,0.9,1,0.9,0.7,0.5,0.2,0)
all_fuzzy_sets <- vector("list",length(all_metar_vars))
names(all_fuzzy_sets) <- paste0("variable_",all_metar_vars)
# Pressure
variable_indices <- which(all_metar_vars %in% c(1))
variable_thresholds <- c(-100,-80,-60,-40,-20,0,20,40,60,80,100)
# all_fuzzy_sets[variable_indices] <- assign_thresholds_and_values(variable_thresholds,similarity_values); rm(variable_indices,variable_thresholds)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)
# Temperature variables
variable_indices <- which(all_metar_vars %in% c(4,10))
variable_thresholds <- c(-5,-4,-3,-2,-1,0,1,2,3,4,5)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)
# Wind speed variables
variable_indices <- which(all_metar_vars %in% c(514,517))
variable_thresholds <- c(-5,-4,-3,-2,-1,0,1,2,3,4,5)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)
# Wind direction
variable_indices <- which(all_metar_vars %in% c(20))
variable_thresholds <- c(-100,-80,-60,-40,-20,0,20,40,60,80,100)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)
# Cloud cover
variable_indices <- which(all_metar_vars %in% c(501,503,505,507))
variable_thresholds <- c(-0.5,-0.4,-0.3,-0.2,-0.1,0,0.1,0.2,0.3,0.4,0.5)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)
# Cloud base height (defined in relative terms!)
variable_indices <- which(all_metar_vars %in% c(502,504,506,508,513))
variable_thresholds <- c(1/2,1/1.8,1/1.6,1/1.4,1/1.2,1,1.2,1.4,1.6,1.8,2)
# variable_thresholds <- c(-500,-400,-300,-200,-100,0,100,200,300,400,500)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)
# Visibility (defined in relative terms!)
variable_indices <- which(all_metar_vars %in% c(515,516))
variable_thresholds <- c(1/2,1/1.8,1/1.6,1/1.4,1/1.2,1,1.2,1.4,1.6,1.8,2)
# variable_thresholds <- c(-1000,-800,-600,-400,-200,0,200,400,600,800,1000)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)
# Hour of the day
variable_indices <- which(all_metar_vars %in% c(888))
variable_thresholds <- c(-5,-4,-3,-2,-1,0,1,2,3,4,5)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)
# Day of the year
variable_indices <- which(all_metar_vars %in% c(889))
variable_thresholds <- c(-100,-80,-60,-40,-20,0,20,40,60,80,100)
all_fuzzy_sets[variable_indices] <- lapply(all_fuzzy_sets[variable_indices],function (x) {setNames(variable_thresholds,similarity_values)}); rm(variable_indices,variable_thresholds)

# Variables c(521,522,745,746,747,748,840) are not given any similarity measures as they are quantitative values and therefore cannot be easily assessed using a numeric value. They are not used while defining similarity values.

# INDIVIDUAL WEIGHTS OF THE VARIABLES IN METARS (HOW ABOUT APPLYING AHP LIKE IN TUBA AND ZOLTAN, 2016 OR SIMPLY TRYING THE WEIGHTS FROM TABLE 1? IN THE PAER, "WITHOUT AHP" -FORECAST REFERS TO EQUAL WEIGHTS FOR THE VARIABLES!)
# Note here that several parameters would be interesting to use in analogy forecasting, but the lack of their availability prevents them to be used
# all_aviation_lists$parameter[match(all_metar_vars,all_aviation_lists$parameter$id),]
variable_weights <- c(2,3,3,4,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,9,NA,2,NA,NA,NA,NA,NA,NA,NA,3,2)
# Normalizing
variable_weights <- variable_weights / max(variable_weights,na.rm=T)
names(variable_weights) <- paste0("variable_",all_metar_vars)

# WEIGHTS FOR THE INDIVIDUAL TIME STEPS (GIVE MOST WEIGHT TO THE LATEST TIMESTEP AND DECREASE RAPIDLY AS THE FURTHER AWAY IN THE PAST THE WEATHER SITUATION IS)
# The equation is taken as provided from Tuba and Bottyan, 2016
number_of_timesteps <- 12
baseline_number <- 1.8
timestep_weights <- (baseline_number^(number_of_timesteps-seq(0,number_of_timesteps-1)-1))/((2^number_of_timesteps)-1)
timestep_weights <- timestep_weights / timestep_weights[1]
# # Plotting the used timestep weighting function
# filename <- paste0("png(\"/data/statcal/results/R_projects/figures/random_figures/analogue_forecasts_time_weighting_baseline_number_",baseline_number,".png\", width = 1800, height = 1200, units = \"px\", pointsize = 12,res=200)")
# eval(parse(text=filename))
# plot(timestep_weights,main=paste0("baseline_number ",baseline_number))
# dev.off()
# rm(filename)
rm(number_of_timesteps)
rm(baseline_number)

# WEIGHTS FOR THE INDIVIDUAL VARIABLE ELEMENTS. Normalizing so that sum*(weight_vector)=1
weight_matrix <- (as.data.frame(lapply(variable_weights, function (x) {x*timestep_weights})))
weight_matrix <-  weight_matrix / sum(weight_matrix,na.rm=T)

# VALUE FROM WHICH DETERMINISTIC FORECASTS ARE DEFINED. WE USE THE MOST SIMILAR SITUATION. The value defined in Tuba and Zoltan, 2016 was "30th percentile of 30 best values -> 10th most similar"
deterministic_value_used <- 1

# HOW LONG FORECASTS ARE GENERATED FROM ANALOGIES? FORECAST TIME IN HOURS. The value below used by Tuba and Zoltan, 2016 was 9 hours. A longer time is used here to allow more direct comparison with NWP products.
forecast_time_in_hours <- 24

# THE TIME PERIOD IN DAYS DEFINING "AUTOCORRELATION TIME". HISTORICAL OBSERVATIONS CLOSER THAN THIS TO THE DAY WHICH FORECASTS ARE PRODUCED TO ARE NOT TAKEN INTO ACCOUNT WHEN SEARCHING FOR ANALOGIES IN THE CROSS-VALIDATED SAMPLE.
# THIS VALUE GUARANNTEES THAT SEMI-INDEPENDENT SAMPLES TO THE ONE BEING FORECAST ARE USED.
proximity_in_days <- 15

# WEIGHT THRESHOLD (in percent) FOR THE DATA THAT NEEDS TO BE INCLUDED WHEN SEARCHING FOR ANALOGIES (IF VARIABLES HAVING SMALLER WEIGHT TO THIS ARE MISSING, THE TIME STAMP IS CONSIDERED AS OK WHEN DEFINING SIMILARITIES FOR ANALOGIES)
# This is not discussed in the Tuba and Zoltan paper (maybe all data is present)
weight_threshold <- 0.001
weight_matrix_boolean <- (weight_matrix > weight_threshold)
weight_matrix_boolean[is.na(weight_matrix_boolean)] <- FALSE

# THE MAXIMUM GAP USED IN THE INTERPOLATION OF OBSERVATIONS (longer gaps are not interpolated)
max_gap_in_hours <- 3

### END



# ASSIGNING RESULT ARRAYS
# Result array covering visibility forecasts for all stations and forecast lengths
# (for converting integers to posixct see with_tz(as.POSIXct(as.integer(dimnames(analogue_forecasts)[[1]]), origin="1970-01-01 00:00:00 UTC"),tzone = "UTC"))
# This is for all stations, but for parallel computing include only one station # analogue_forecasts <- array(NA,dim = c(length(all_forecast_times),length(all_station_ids),forecast_time_in_hours+1))
analogue_forecasts <- array(NA,dim = c(length(all_forecast_times),1,forecast_time_in_hours+1))
dimnames(analogue_forecasts)[[1]] = as.POSIXct(all_forecast_times)
# dimnames(analogue_forecasts)[[2]] = all_station_ids
dimnames(analogue_forecasts)[[2]] = 1
dimnames(analogue_forecasts)[[3]] = seq(0,forecast_time_in_hours)

# Result array covering data availability for all stations/variables
all_available_data <- as.data.frame(matrix(NA, nrow = length(all_station_ids), ncol = length(all_metar_vars)))
dimnames(all_available_data)[[1]] <- all_station_ids
dimnames(all_available_data)[[2]] <- all_metar_vars
# Result array covering INTERPOLATED data availability for all stations/variables
all_available_data_interpolated <- as.data.frame(matrix(NA, nrow = length(all_station_ids), ncol = length(all_metar_vars)))
dimnames(all_available_data_interpolated)[[1]] <- all_station_ids
dimnames(all_available_data_interpolated)[[2]] <- all_metar_vars
# Result vector covering data availability for all stations (timesteps with full training/forecast data available)
all_available_data_timesteps_no <- rep(NA,length(all_station_ids))
names(all_available_data_timesteps_no) <- all_station_ids

# Going through one station at a time
# station_id_no <- 20 # Tampere Pirkkala
foreach (station_id_no=c(8,16,20,19,26,11,24,22,6,7,18,1,4,13,2,5,29,3)) %dopar% {
  print(station_id_no)
      
  ### Loading sorted METAR station data ###
  data_file <- paste0(data_directory,"station_id_",station_id_no,"_sorted.RData")
  station_data <- loadRData(data_file)
  rm(data_file)
  
  # How much data does each variable have? Saving data availability to matrix covering all stations
  available_data <- unlist(lapply(station_data[,-1],function (x) {sum(!is.na(x))})) / do.call(dim,list(x=station_data))[1] * 100
  all_available_data[match(station_id_no,all_station_ids), match(names(available_data),dimnames(all_available_data)[[2]])] <- available_data
  save(file=paste0(data_directory_output,"station_id_",station_id_no,"_available_data_pct"),list="available_data")
  rm(available_data)
  
  # Interpolating missing values using LINEAR interpolation
  station_data[,-1] <- lapply(station_data[,-1],function(x) {zoo::na.approx(x,maxgap=max_gap_in_hours*2,na.rm=FALSE)})

  # How much data does each INTERPOLATED variable have? Saving data availability to matrix covering all stations
  available_data <- unlist(lapply(station_data[,-1],function (x) {sum(!is.na(x))})) / do.call(dim,list(x=station_data))[1] * 100
  all_available_data_interpolated[match(station_id_no,all_station_ids), match(names(available_data),dimnames(all_available_data_interpolated)[[2]])] <- available_data
  save(file=paste0(data_directory_output,"station_id_",station_id_no,"_available_data_interpolated_pct"),list="available_data")
  rm(available_data)
  
  # If data is completely empty, remove loaded data vector and move onto next month
  if (do.call(dim,list(x=station_data))[1]==0 | (mean(unlist(lapply(station_data[,-1],function (x) {sum(is.na(x))}))) == do.call(dim,list(x=station_data))[1])) {
    rm(data_file)
    rm(station_data)
    next
  } else {

    # Making simple variable conversions (D2 <- T2-D2)
    station_data[["10"]] <- station_data[["4"]] - station_data[["10"]]
    # Storing hour of the day to the second last column
    station_data[["888"]] <- as.integer(format(station_data[,1],"%H"))
    # Storing day of the year to the last column
    station_data[["889"]] <- daydoy(station_data[,1])

    # When a data time stamp is used for forecasting, it needs to contain both the training (length(timestep_weights)) and forecasting (forecast_time_in_hours) data. Forecasts can be done to all those time stamps which have training period available.
    # Calculating the number of time stamps with full training+forecasting data available.
    ### something like this would likely be much faster...apply(station_data[((length(timestep_weights)+1):(dim(station_data)[1]-forecast_time_in_hours)),(which(!is.na(variable_weights))+1)],1,function (x) {sum(!is.na(x[]))})
    training_rows <- rep(NA,dim(station_data)[1])
    for (row in (length(timestep_weights)):(dim(station_data)[1]-2*forecast_time_in_hours)) {
      training_rows[row] <- (sum(is.na(station_data[((row-length(timestep_weights)+1):(row+2*forecast_time_in_hours)),(which(!is.na(variable_weights))+1)]))==0)
      # print(row)
    }
    rm(row)
    # Saving the number of training data rows to result matrix
    available_data_timesteps_no_station <- sum(training_rows==TRUE,na.rm=TRUE)
    all_available_data_timesteps_no[which(names(all_available_data_timesteps_no)==station_id_no)] <- available_data_timesteps_no_station
    save(file=paste0(data_directory_output,"station_id_",station_id_no,"_available_data_timesteps_no.RData"),list="available_data_timesteps_no_station")
    rm(available_data_timesteps_no_station)
    
    # Creating similarity matrix for the station (every time stamp has a mutual similarity so that loop has to calculate this similarity only once)
    # Rows in the similarity matrix correspond to dates being predicted, and columns to all training_rows which are available to create forecasts
    similarity_matrix <- matrix(data=NA,nrow=length(all_forecast_times),ncol=sum(training_rows==TRUE,na.rm=T))
    dimnames(similarity_matrix)[[1]] <- match(all_forecast_times,station_data$obstime)
    dimnames(similarity_matrix)[[2]] <- which(training_rows==TRUE)
    
    # Going through every data point in forecast_data (all_forecast_times indicate dates which are forecasted)
    for (row1 in match(all_forecast_times,station_data$obstime)) {
      # row1 <- match(all_forecast_times,station_data$obstime)[2030] # aja ristiin indeksit 298075 (488) ja 307327 (2030)
      print(row1)
      # Assign training data
      if (row1>length(timestep_weights)) {
        training_data <- station_data[((row1-length(timestep_weights)+1):row1),(which(!is.na(variable_weights))+1)]
      } else {
        next
      }
      # If time does not have THE WHOLE training data available, proceed to the next time
      if (sum(is.na(training_data))!=0) {
        rm(training_data)
        next
      } else {
        # print("moi!")
        # Sys.sleep(500)
        # Only using training data which are further away than proximity_in_days
        training_rows2 <- which(training_rows==TRUE)
        training_rows2 <- training_rows2[which(abs(difftime(station_data$obstime[training_rows2],station_data$obstime[row1],units="secs"))>(proximity_in_days*24*3600))]
        # # Assigning matrix for the similarity index
        # similarity_indices <- rep(NA,length(training_rows2))
        # Going through training rows one-by-one)
        for (row2 in training_rows2) {
          print(row2)
          # Only proceeding if the similarity between these two stations has not been calculated earlier
          if (is.na(similarity_matrix[match(row1,dimnames(similarity_matrix)[[1]]),match(row2,dimnames(similarity_matrix)[[2]])])) {
            training_data_history <- station_data[((row2-length(timestep_weights)+1):row2),(which(!is.na(variable_weights))+1)]
            training_data_difference <- NA*training_data
            # Variables with similarities defined using absolute difference
            columns <- which(names(training_data_difference) %in% c(1,4,10,514,517,501,503,505,507))
            training_data_difference[,columns] <- training_data[,columns] - training_data_history[,columns]
            # Variables with similarities defined using relative difference
            columns <- which(names(training_data_difference) %in% c(502,504,506,508,513,515,516))
            training_data_difference[,columns] <- training_data[,columns] / training_data_history[,columns]
            # Wind direction
            columns <- which(names(training_data_difference) %in% c(20))
            training_data_difference[,columns] <- circular_variable_difference(training_data[,columns],training_data_history[,columns],360)
            # Hour of the day
            columns <- which(names(training_data_difference) %in% c(888))
            training_data_difference[,columns] <- circular_variable_difference(training_data[,columns],training_data_history[,columns],24)
            # Day of the year
            columns <- which(names(training_data_difference) %in% c(889))
            training_data_difference[,columns] <- circular_variable_difference(training_data[,columns],training_data_history[,columns],365)
            
            # Interpolating similarity values between those defined initially in the script (two lists in mapply: fuzzy sets and training_data_difference)
            training_data_difference <- mapply(function (x,y) approx(x,similarity_values,y,yleft=0,yright=0)[[2]], x=all_fuzzy_sets[which(!is.na(variable_weights))], y=training_data_difference)
            # Weighting the variable/time step -dependent similarity values and assigning to similarity matrix
            # Saving value to both possible places: The first is always saved as they are defined by row1 (time stamp defined in all_forecast_times) and row2 (time stamp defined in training_rows2). The second is saved only on some cases as it has requirements: 1) row2 has to be among all_forecast_times and 2) timestep row1 has to have also future data for the forecast_time_in_hours 3) row2 cannot get values in the vicinity of row1 in the first place, as they are constrained by proximity_in_days
            similarity_matrix[match(row1,dimnames(similarity_matrix)[[1]]),match(row2,dimnames(similarity_matrix)[[2]])] <- sum(weight_matrix[(dim(weight_matrix)[1]:1),which(!is.na(variable_weights))] * training_data_difference)
            if (!is.na(match(row2,dimnames(similarity_matrix)[[1]])) & !is.na(match(row1,dimnames(similarity_matrix)[[2]]))) {
              similarity_matrix[match(row2,dimnames(similarity_matrix)[[1]]),match(row1,dimnames(similarity_matrix)[[2]])] <- sum(weight_matrix[(dim(weight_matrix)[1]:1),which(!is.na(variable_weights))] * training_data_difference)
            }
          } else {
            next
          }
        }
        rm(row2)
      }
    }
    rm(row1)
    
    # plot(similarity_matrix[33,])
    # station_data$obstime[as.numeric(dimnames(similarity_matrix)[[2]][which(is.na(similarity_matrix[33,]))])]
    # station_data$obstime[as.numeric(dimnames(similarity_matrix)[[1]][33])]
    
    # FROM THE CALCULATED SIMILARITY MATRIX, SELECT THE DETERMINISTIC FORECASTS MOST SIMILAR TO CURRENT SITUATION
    for (forecast_time_stamp in 1:length(all_forecast_times)) {
      station_data_index <- as.numeric(dimnames(similarity_matrix)[[2]][which(similarity_matrix[forecast_time_stamp,]==sort(similarity_matrix[forecast_time_stamp,],decreasing=TRUE,na.last=TRUE)[deterministic_value_used])])
      if (!length(station_data_index)==FALSE) {
        # analogue_forecasts[forecast_time_stamp,all_station_ids[station_id_no],] <- station_data$`515`[station_data_index:(station_data_index+forecast_time_in_hours)]
        analogue_forecasts[forecast_time_stamp,1,] <- station_data$`515`[station_data_index:(station_data_index+forecast_time_in_hours)]
      }
      rm(station_data_index)
    }
    rm(forecast_time_stamp)
    
    # Making analogue forecasts for the station into same format as other model data and save it to a file
    analogue_forecasts_station <- melt(analogue_forecasts[,1,])
    analogue_forecasts_station[,2] <- analogue_forecasts_station[,1] + analogue_forecasts_station[,2]*3600
    analogue_forecasts_station <- analogue_forecasts_station[order(analogue_forecasts_station[,1],analogue_forecasts_station[,2]),]
    analogue_forecasts_station[,1:2] <- lapply(analogue_forecasts_station[,1:2],function(x) {with_tz(as.POSIXct(as.integer(x), origin="1970-01-01 00:00:00 UTC"),tzone = "UTC")})
    analogue_forecasts_station <- cbind(station_id_no,analogue_forecasts_station[,1:2],10,515,analogue_forecasts_station[,3])
    dimnames(analogue_forecasts_station)[[2]] <- c("station_id","analysis_time","forecast_time","model_id","parameter","value")
    save(file=paste0(data_directory_output,"station_id_",station_id_no,"_analogies.RData"),list="analogue_forecasts_station")
    rm(analogue_forecasts_station)
    
  }

}
rm(station_id_no)

# save(file=paste0(data_directory_output,"all_stations_analogies.RData"),list="analogue_forecasts")
# save(file=paste0(data_directory_output,"all_available_data_pct.RData"),list="all_available_data")
# save(file=paste0(data_directory_output,"all_available_data_interpolated_pct.RData"),list="all_available_data_interpolated")
# save(file=paste0(data_directory_output,"all_available_data_timesteps_no.RData"),list="all_available_data_timesteps_no")