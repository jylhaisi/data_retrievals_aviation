malliarvot_2017 <- model_VIS_2017_01_allstations

malliarvot_2017$value <- as.numeric(gsub("\\{|\\}", "", malliarvot_2017$value))
# Voit ihan hyvin käsitellä pelkkiä ennustepituuksia
malliarvot_2017$forecast_time <- as.numeric(round(difftime(malliarvot_2017$forecast_time,malliarvot_2017$analysis_time,units="hours")))
names(malliarvot_2017)[3] <- "forecast_length"
# Toistat vaan tämän kaltaisen käsittelyn joka mallille (ec_natiivi,ec_metku,har_natiivi,har_metku)
malli1 <- subset(subset(malliarvot_2017, model_id==5),parameter_id==1232)
malli1 <- malli1[,-c(4:5)]
# Tätä tarkoitin arraylla. Helpottaa jatkokäsittelyä ihan suunnattoman paljon. Kun uniikit timestampit määrätään malli1:n perusteella, ovat loppuarrayt kaikki samankokoisia!
malli1_sorted <- array(NA,dim=c(length(unique(malli1$analysis_time)),length(seq(0,24)),length(unique(malli1$station_id))))
# POSIXct-muoto muuttuu dataframeihin sijoitettuna pelkiksi numeroiksi, mutta jos haluat tarkastella kuukausia erikseen tallenna tämä vaikkapa omaan muuttujaansa
dimnames(malli1_sorted)[[1]] <- unique(malli1$analysis_time)
dimnames(malli1_sorted)[[2]] <- seq(0,24)
dimnames(malli1_sorted)[[3]] <- unique(malli1$station_id)
for (i in 1:length(seq(0,24))) {
  for (j in 1:length(unique(malli1$station_id))) {
    assigned <- (subset(malli1,forecast_length==seq(0,24)[i] & station_id==unique(malli1$station_id)[j]))
    if (!length(assigned)==FALSE & !length(match(assigned$analysis_time,unique(malli1$analysis_time)))==FALSE) {
      malli1_sorted[match(assigned$analysis_time,unique(malli1$analysis_time)),i,j] <- assigned$value
    }
  }
}
# esim. aseman 16 arvot ovat tässä
malli1_sorted[,,dimnames(malli1_sorted)[[3]]==16]


malli1 <- subset(subset(subset(malliarvot_2017, model_id==5),parameter_id==1232))
rm(list=ls()[grep("data",ls())])
rm(list=ls()[grep("model_VIS",ls())])
malli1_value <- as.numeric(gsub("\\{|\\}", "", malli1$value))
new_data_frame <- data.frame(malli1$analysis_time, malli1$forecast_time,malli1$station_id,malli1_value)
dimnames(new_data_frame)[[2]] <- c("analysis_time", "forecast_time","station_id", "value")
aikamatriisi_malli3h <- seq(from=as.POSIXct("2017-1-1 00:00:00", tz="GMT"), to=as.POSIXct("2018-01-01 00:00:00", tz="GMT"), by="3 hour")
aikamatriisi_tuplattuna3h <- as.data.frame(rep(aikamatriisi_malli3h,each=3)) # "tuplaa" jokaisen ajanhetken kerrottuna 3
forecast_time3h <- seq(from=as.POSIXct("2017-1-1 00:00:00", tz="GMT"), to=as.POSIXct("2018-01-01 02:00:00", tz="GMT"), by="1 hour")
#luodaan taulukko (samankaltainen kuin metareissa) ja sijoitetaan jokaisen aseman kohdalle "NA"
taulukko3h <- as.data.frame(matrix(NA,nrow=3*length(aikamatriisi_malli3h),ncol=dim(asemaluettelo)[1]+2))
taulukko3h[,1] <- aikamatriisi_tuplattuna3htaulukko3h[,2] <- forecast_time3hdimnames(taulukko3h)[[2]] <- c("analysis_time","forecast_time",asemaluettelo$id)
library(data.table)
# käydään jokainen asema läpi datasta
for (zarake in 3:length(taulukko3h)) {jarjestetty3h <- setDT(subset(new_data_frame,station_id==names(taulukko3h)[zarake]))[order(analysis_time, forecast_time)][, head(.SD, 3L), by = analysis_time]taulukko3h[,zarake] <- jarjestetty3h$value[match(taulukko3h$forecast_time, jarjestetty3h$forecast_time)]
  rm(jarjestetty3h)
  print(zarake)
}
