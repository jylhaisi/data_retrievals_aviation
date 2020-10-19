# Tämä skripti lataa aviation-tietokannasta METAR-havaintoja sekä eri malleille näkyvyysennusteita.
rm(list=ls())

# Välttämättömät asema- ja muuttujalistat yms.
source("load_libraries_tables_and_open_connections.R")
source("hae_aviation_scripts.R")

# Lentokentat
asemalista_suomalaiset_lentokentat <- read.csv("suomalaiset_lentokentat.csv",header=TRUE)
asemalista_suomalaiset_lentokentat <- asemalista_suomalaiset_lentokentat[,-1]
previ_ecmos_v_station_id <- asemalista_suomalaiset_lentokentat$wmon
asemajoukko <- paste("suomalaiset_lentokentat",sep="")

station_id <- "27"
if (station_id=="all") {
  station_tarkenne <- "allstations"
} else {
  station_tarkenne <- paste0("station_id_",station_id)
}

# Data is retrieved in one month chunks, as otherwise the extent would be too much
for (year in 2019:2019) {
  for (month in 6:6) {
    if (month<10) {
      month1 <- paste("0",month,sep="")
    } else {
      month1 <- month
    }
    year1 <- year
    if (month==12) {
      month2 <- "01"
      year2 <- year1 + 1
    } else if (month<9) {
      month2 <- paste("0",(month + 1),sep="")
      year2 <- year1
    } else {
      month2 <- month + 1
      year2 <- year1
    }
    
    # Bypassing the loop and manually setting the retrieval interval here
    month1 <- "05"
    month2 <- "10"
    year1 <- "2019"
    year2 <- "2019"
    
    
    ### MANUAALISEN METAR-DATAN HAKU AVIATION-TIETOKANNASTA ###
    
    # Tämän pitäisi olla ihan no-brainer -homma. Manuaalihavainnot on parsittu suoraan METAR:eista ja niitä on v. 2005 lähtien. Tekstimuotoiset METAR:it on haettu samasta taulusta kuin TAF:it
    Id <- c(1,4,10,20,501,502,503,504,505,506,507,508,513,514,515,516,517,521,522,523,745,746,747,748) # c(1,4,10,20,501,502,513,514,515,516,517,521,522,523) # Id <- 515 # (Aviation visibility M)
    sel_AQU <- "all"
    table_form <- "wide" # joko leveä (wide) tai kapea (mikä hyvänsä muu parametri)
    # message_type 1 on manuaalihavainnot, numero 8 autohavainnot, 0 on SPECI-havainnot
    for (message_type_id in c(1)) { # (message_type_id in c(0,1,8)) {
      saved_variable <- paste("havainnot_METAR_",year1,"_",month1,"_",message_type_id,"_",station_tarkenne,sep="")
      filename <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/EFHK/",saved_variable,".csv",sep="")
      eval(subs(saved_variable <- hae_havainnot_aviation(sel_AQU,station_id,message_type_id,month1,month2,year1,year2)))
      # # Write in RData-form
      # eval(subs(save(saved_variable,file=filename,compress=TRUE)))
      # write in csv-form
      komento = paste0("write_csv(",saved_variable,",\"",filename,"\")")
      eval(parse(text=komento))
      rm(komento)
      eval(subs(rm(saved_variable)))
    }
    rm(message_type_id)
    rm(sel_AQU)
    rm(Id)
    
    
    # Haetaan pelkät tekstimuotoiset METAR:it
    for (message_type_id in c(1)) { # (message_type_id in c(0,1,8)) {
      saved_variable <- paste("teksti_METAR_",year1,"_",month1,"_",message_type_id,"_",station_tarkenne,sep="")
      filename <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/EFHK/",saved_variable,".csv",sep="")
      eval(subs(saved_variable <- hae_sanomat_aviation(station_id,message_type_id,month1,month2,year1,year2)))
      # # Write in RData-form
      # eval(subs(save(saved_variable,file=filename,compress=TRUE)))
      # write in csv-form
      komento = paste0("write_csv(",saved_variable,",\"",filename,"\")")
      eval(parse(text=komento))
      rm(komento)
      eval(subs(rm(saved_variable)))
    }
    rm(month1)
    rm(month2)
    rm(year1)
    rm(year2)
  }
  rm(month)
}
rm(year)












# ### MALLIENNUSTEIDEN HAKU AVIATION-TIETOKANNASTA ###
# 
# for (year in 2017:2017) {
#   for (month in c(8:12)) {
#     if (month<10) {
#       month1 <- paste("0",month,sep="")
#     } else {
#       month1 <- month
#     }
#     year1 <- year
#     if (month==12) {
#       month2 <- "01"
#       year2 <- year1 + 1
#     } else if (month<9) {
#       month2 <- paste("0",(month + 1),sep="")
#       year2 <- year1
#     } else {
#       month2 <- month + 1
#       year2 <- year1
#     }
# 
#     # Näkyvyys HIMAN + mallin oma kaikki asemat kaikki mallit
#     saved_variable <- paste("model_VIS_",year1,"_",month1,"_allstations",sep="")
#     filename <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/",saved_variable,".RData",sep="")
#     variables <- c(1232,10407,407,515)
#     models <- c(1,2,3,4,5)
#     eval(subs(saved_variable <- hae_model_aviation(station_id,variables,models,month1,month2,year1,year2)))
#     eval(subs(save(saved_variable,file=filename,compress=TRUE)))
#     eval(subs(rm(saved_variable)))
# 
#     # Lämpötila+dewpoint+tuuli+sade kaikki asemat Harmonie+EC
#     saved_variable <- paste("model_othervars_",year1,"_",month1,"_allstations",sep="")
#     filename <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/",saved_variable,".RData",sep="")
#     variables <- c(4,10,21,50)
#     models <- c(1,5)
#     eval(subs(saved_variable <- hae_model_aviation(station_id,variables,models,month1,month2,year1,year2)))
#     eval(subs(save(saved_variable,file=filename,compress=TRUE)))
#     eval(subs(rm(saved_variable)))
# 
#     # Pilven alaraja
#     saved_variable <- paste("model_CLB_",year1,"_",month1,"_allstations",sep="")
#     filename <- paste("/data/statcal/results/R_projects/data_retrievals_aviation/",saved_variable,".RData",sep="")
#     variables <- c(500,1229)
#     models <- c(1,2,3,4,5)
#     eval(subs(saved_variable <- hae_model_aviation(station_id,variables,models,month1,month2,year1,year2)))
#     eval(subs(save(saved_variable,file=filename,compress=TRUE)))
#     eval(subs(rm(saved_variable))
#     
#     
#     rm(month1)
#     rm(month2)
#     rm(year1)
#     rm(year2)
#   }
#   rm(month)
# }
# rm(year)



rm(station_id)
rm(con3)
rm(drv)
