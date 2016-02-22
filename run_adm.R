library(data.table);library(bit64);library(plyr)
suppressMessages(suppressWarnings(library(RMySQL)))


path_client="C:\\Users\\yuemeng1\\Desktop\\code\\dashboard\\sample" #ensure no \\ at the end of the path

#is.staging=T  True is to staging DB and F is to production DB
#update=T if you want to delete all the info from this client, or it is a new client, put it as T

setwd(path_client)
final=fread("input_modelinput_data.csv")
datelkup=fread("input_date_lkup.csv")
varlkup=fread("input_varlkup.csv",na.strings="")
dmalkup=fread("input_dmalkup.csv")
setin=fread("input_setup.csv")
homesetup=fread("input_home_setup.csv")
typetable=fread("input_type.csv")
datelkup$week=as.Date(datelkup$week,"%m/%d/%Y")

#Please check if the date format makes sense after this step
final$week=as.Date(final$week,"%m/%d/%Y")

if(homesetup$update ==1) update=T else update=F
if(homesetup$is.staging==1) is.staging=T else is.staging=F
if(homesetup$is.new.client==1) is.new.client=T else is.new.client=F

# DB server info
db.name="nviz"
port=3306
if (is.staging){
  db.server="127.0.0.1"
  username="root"
  password="bitnami"
}else{
  db.server="bitnami.cluster-chdidqfrg8na.us-east-1.rds.amazonaws.com"
  username="Zkdz408R6hll"
  password="XH3RoKdopf12L4BJbqXTtD2yESgwL$fGd(juW)ed"
}

conn <- dbConnect(MySQL(),user=username, password=password,dbname=db.name, host=db.server)


path_code=paste(path_client,"R",sep="\\")
#########################
#Transform and uploading#
#########################

setwd(path_code)
source("adm_transform.R",local=F)
source("adm_update.R",local=F)
adm_update()