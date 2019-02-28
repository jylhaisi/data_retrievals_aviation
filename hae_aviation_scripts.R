# Nämä funktiot hakevat lentosäähavaintoja/mallikamaa aviation-kannasta kuukausi kerrallaan

hae_havainnot_aviation <- function(sel_AQU,station_id,message_type_id,month1,month2,year1,year2) {
  
  if (sel_AQU=="all") {
    sel_AQU <- paste("",sep="")
  } else {
    sel_AQU <- paste(sel_AQU,collapse = "','")
    sel_AQU <- paste("and p.id IN ('",sel_AQU,"')",sep="")
  }
  message_type_id <- paste(message_type_id,collapse = "','")
  havainnot <- as.data.frame(matrix(NA,0,0))
  
  # Haetaan Teron AQU-tietokannasta METAR-havaintoja lentokentille ja tallennetaan tämä tieto muuttujaan havainnot
  # HUOM! MessagetypeId=1 ottaa huomioon vain manuaaliset METARIT, auto-METARIt ovat arvolla 8!
  if (station_id=="all") {
    sql_query <- paste0("select mc.station_id, mc.ttime, mc.message_type_id, p.id, mv.rvalue from met_value mv, parameter p, message_content mc, station s where mc.id=mv.parent_id and s.id=mc.station_id and p.id=mv.parameter_id and mc.ttime between to_date('",year1,month1,"010001','YYYYMMDDHH24MI') and to_date('",year2,month2,"010000','YYYYMMDDHH24MI') ",sel_AQU," and mc.message_type_id IN ('",message_type_id,"');")
  } else {
    sql_query <- paste0("select mc.station_id, mc.ttime, mc.message_type_id, p.id, mv.rvalue from met_value mv, parameter p, message_content mc, station s where mc.id=mv.parent_id and s.id=mc.station_id and p.id=mv.parameter_id and mc.ttime between to_date('",year1,month1,"010001','YYYYMMDDHH24MI') and to_date('",year2,month2,"010000','YYYYMMDDHH24MI') ",sel_AQU," and s.id=",station_id," and mc.message_type_id IN(",message_type_id,");")
  }
  # print(sql_query)
  haku <- dbSendQuery(con4, sql_query)
  havainnot <- fetch(haku)
  rm(sql_query)
  rm(haku)
  
  # Tähän asti havaintoja voidaan hakea, vaikkei niitä tietokannassa olisikaan. Jos aseman osalta ei ole ollenkaan havaintoja, skipataan skriptin jatkokäsittely.
  if (dim(havainnot)[1]!=0) {
    # Ordering query
    havainnot <- havainnot[order(havainnot$station_id,havainnot$message_type_id,havainnot$ttime,havainnot$id),]
    
    # Poistetaan ne ajanhetket, joille on puuttuvat arvot (näitäkin esiintyy tietokannassa)
    if (dim(havainnot)[1]>0) {
      havainnot <- havainnot[!is.na(havainnot$rvalue),]
    }
    names(havainnot) <- c("station_id","obstime","message_type_id","param_id","value")
    # Siistitään havaintomatriisin sarakkeet kunnolliseen muotoon
    havainnot$obstime <- as.POSIXct(as.character(havainnot$obstime),tz="GMT")
    # Muutetaan varmuuden vuoksi vielä numeeriseen muotoon (Brainstormista haettavat eivät sitä ole)
    havainnot$value <- as.numeric(as.character((havainnot$value)))
    
    
    # TÄTÄ KOHTAA VOISI OPTIMOIDA MELKO TAVALLA VIELÄ!!!
    # Joillekin havaintohetkille voi löytyä duplikaattiarvoja. Otetaan duplikaattiarvot pois siten, että jätetään jäljelle useiden saman ajanhetken havaintojen keskiarvo (koska ei voida tietää miltä ajanhetkeltä pyöristetyt ajanhetket ovat peräisin)
    # # Ensin täytyy sortata aikajärjestykseen, jos data ei jostain syystä olisikaan sellaisessa.
    # havainnot <- havainnot[order(havainnot$obstime),]
    # Tämän jälkeen käydään yksi kerrallaan läpi vektoria ja keskiarvoistetaan duplikaattiarvot yhden desimaalin tarkkuuteen (tai jos on jopa useampia, niin kaikki saman ajanhetken arvot)
    if (sum(duplicated(havainnot))>0) {
      havainnot <- havainnot[-which(duplicated(havainnot)),]
      # for (l in 2:dim(havainnot)[1]) {
      #   if (l <= dim(havainnot)[1]) {
      #     if (havainnot$obstime[l]==havainnot$obstime[l-1]) {
      #       alkurivi <- (l-1)
      #       loppurivi <- tail(which(havainnot$obstime==havainnot$obstime[l-1]),n=1)
      #       # Sijoitetaan viimeiseen alkioon kaikkien saman ajanhetken arvojen keskiarvo ja poistetaan kaikki viimeistä edeltävät arvot.
      #       havainnot$value[loppurivi] <- round(mean(havainnot$value[alkurivi:loppurivi],na.rm=T),digits=1)
      #       havainnot <- havainnot[-(alkurivi:(loppurivi-1)),]
      #       rm(alkurivi)
      #       rm(loppurivi)
      #     }
      #   }
      # }
      # rm(l)
    }
    
    
  }

  havainnot
}

hae_sanomat_aviation <- function(station_id,month1,month2,year1,year2) {
  
  METAR_teksti <- as.data.frame(matrix(NA,0,0))
  
  # Haetaan Teron AQU-tietokannasta METAR-havaintoja lentokentille ja tallennetaan tämä tieto muuttujaan havainnot
  # HUOM! MessagetypeId=1 ottaa huomioon vain manuaaliset METARIT, auto-METARIt ovat arvolla 8!
  if (station_id=="all") {
    sql_query <- paste0("select mc.station_id, mc.ttime, message_type_id, mc.content from message_content mc where ttime between to_date('",year1,month1,"010001','YYYYMMDDHH24MI') and to_date('",year2,month2,"010000','YYYYMMDDHH24MI') and message_type_id IN (0,1,8);")
  } else {
    sql_query <- paste0("select mc.station_id, mc.ttime, message_type_id, mc.content from message_content mc where ttime between to_date('",year1,month1,"010001','YYYYMMDDHH24MI') and to_date('",year2,month2,"010000','YYYYMMDDHH24MI') and mc.station_id=",station_id," and message_type_id IN (0,1,8);")
  }
  haku <- dbSendQuery(con4, sql_query)
  METAR_teksti <- fetch(haku)
  rm(haku)
  
  # Tähän asti havaintoja voidaan hakea, vaikkei niitä tietokannassa olisikaan. Jos aseman osalta ei ole ollenkaan havaintoja, skipataan skriptin jatkokäsittely.
  if (dim(METAR_teksti)[1]!=0) {
    names(METAR_teksti) <- c("station_id","obstime","message_type_id","content")
    # Ordering query
    METAR_teksti <- METAR_teksti[order(METAR_teksti$station_id,METAR_teksti$message_type_id,METAR_teksti$obstime),]
    
    # Siistitään havaintomatriisin sarakkeet kunnolliseen muotoon
    METAR_teksti$obstime <- as.POSIXct(as.character(METAR_teksti$obstime),tz="GMT")
    # Removing duplicates
    if (sum(duplicated(METAR_teksti))>0) {
      METAR_teksti <- METAR_teksti[-which(duplicated(METAR_teksti)),]
    }
  }
  
  METAR_teksti
}

hae_model_aviation <- function(station_id,variables,models,month1,month2,year1,year2) {
  
  variables <- paste(variables,collapse = ",")
  models <- paste(models, collapse = ",")
  
  if (station_id=="all") {
    sql_query <- paste0("select l.station_id, mr.origintime, mtp.validtime, mr.model_id, mv.parameter_id, mv.rvalue from model_value mv, model_time_position mtp, modelrun mr, location l where mr.origintime between to_date('",year1,month1,"010001','YYYYMMDDHH24MI') and to_date('",year2,month2,"010000','YYYYMMDDHH24MI') and mv.parameter_id in (",variables,") and l.level_type_id=1 and mr.model_id in (",models,") and mtp.modelrun_id=mr.id and mv.parent_id=mtp.id and mtp.location_id=l.id;")
  } else {
    sql_query <- paste0("select l.station_id, mr.origintime, mtp.validtime, mr.model_id, mv.parameter_id, mv.rvalue from model_value mv, model_time_position mtp, modelrun mr, location l where mr.origintime between to_date('",year1,month1,"010001','YYYYMMDDHH24MI') and to_date('",year2,month2,"010000','YYYYMMDDHH24MI') and mv.parameter_id in (",variables,") and l.level_type_id=1 and mr.model_id in (",models,") and mtp.modelrun_id=mr.id and mv.parent_id=mtp.id and mtp.location_id=l.id and l.id=",station_id,";")
  }
  haku <- dbSendQuery(con4, sql_query)
  VIS_all_Har_EC <- fetch(haku)
  rm(haku)
  
  # Tähän asti havaintoja voidaan hakea, vaikkei niitä tietokannassa olisikaan. Jos aseman osalta ei ole ollenkaan havaintoja, skipataan skriptin jatkokäsittely.
  if (dim(VIS_all_Har_EC)[1]!=0) {
    names(VIS_all_Har_EC) <- c("station_id","analysis_time","forecast_time","model_id","parameter_id","value")
    # Ordering query
    VIS_all_Har_EC <- VIS_all_Har_EC[order(VIS_all_Har_EC$station_id,VIS_all_Har_EC$analysis_time,VIS_all_Har_EC$forecast_time,VIS_all_Har_EC$model_id,VIS_all_Har_EC$parameter_id),]
    
    # Siistitään havaintomatriisin sarakkeet kunnolliseen muotoon
    VIS_all_Har_EC$analysis_time <- as.POSIXct(as.character(VIS_all_Har_EC$analysis_time),tz="GMT")
    VIS_all_Har_EC$forecast_time <- as.POSIXct(as.character(VIS_all_Har_EC$forecast_time),tz="GMT")
    # Removing duplicates
    if (sum(duplicated(VIS_all_Har_EC))>0) {
      VIS_all_Har_EC <- VIS_all_Har_EC[-which(duplicated(VIS_all_Har_EC)),]
    }
  }
  
  VIS_all_Har_EC
}