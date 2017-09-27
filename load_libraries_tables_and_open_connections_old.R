# Ensin täytyy ladata sisään PostgreSQL- sekä datan käsittelyssä tarvittavat kirjastot
library(RPostgreSQL)
library(car)
library(MASS)
require(mgcv)
library(xlsx)
library(verification)
library(scrapeR)
library(prodlim)
library(insol)
library(abind)
library(insol)
library(pryr)
# library(Hmisc)

AQU_muuttujat <- read.csv("AQU_parameter_distinct_csv.txt", sep=",",header = TRUE,stringsAsFactors = FALSE)

# Ladataan mallin asemajoukkolista (wmon) ja otetaan pois sen viimeinen rivi (footer).
previ_ecmos_v_station_id <- read.csv("previ_ecmos_v_uniikit_station_id.txt",header = TRUE,stringsAsFactors = FALSE)
previ_ecmos_v_station_id <- previ_ecmos_v_station_id[-(dim(previ_ecmos_v_station_id)[1]),]

# Ladataan perustellulla tavalla typistetty wmostations-taulu, jotta voidaan tehdä konversio wmon -> fmisid (Havainnot ovat fmisid-tunnisteella). Tässä listassa olevien havaintoasemien asianmukaisuus on suodatettu läpi skriptissä (crop_wmostations_list_to_homogeneous.R)
# TÄMÄ TAULU SISÄLTÄÄ KURANTIN KÄYTETTÄVISSÄ OLEVAN DATAN, JOLLE HAVAINTOASEMIEN SIJAINNIT MOS:N INTERPOLOINTIAJANHETKELLÄ OVAT YHTENEVÄISET NIIDEN ASEMASIJAINTIEN KANSSA, JOTKA OVAT KOKO MOS-KOULUTUSAJANJAKSOLLA HOMOGEENISET.
station_idt_conversion <- read.csv("wmostations_cropped_station_list.csv",header = TRUE,stringsAsFactors = FALSE)
station_idt_conversion <- station_idt_conversion[,-1 ,drop= FALSE]

# Lentokentat
asemalista_suomalaiset_lentokentat <- read.csv("suomalaiset_lentokentat.csv",header=TRUE)
asemalista_suomalaiset_lentokentat <- asemalista_suomalaiset_lentokentat[,-1]

previ_ecmos_v_station_id <- station_idt_conversion$wmon

# Otetaan yhteys PostgreSQL-tietokantaan
# Alustetaan ajuri niin, että saadaan haettua max. 20000000 riviä kerrallaan.
drv <- dbDriver("PostgreSQL",fetch.default.rec=50000000)
# Otetaan yhteys mos-tietokantaan.
con3 <- dbConnect(drv, host="vorlon.fmi.fi", user="ylhaisi", password="6Mgk9fe3Hsz7", dbname="aviation")