library(data.table);library(bit64);library(plyr)
suppressMessages(suppressWarnings(library(RMySQL)))


# folder where you put your 7za and pscp
path_system="C:\\Users\\yuemeng1\\"
# folder where you put your key for AWS RDS
path_key="C:\\Users\\yuemeng1\\Desktop\\Github\\ninah.ppk"
# folder where you put all input files for the client
path_client="C:\\Users\\yuemeng1\\Desktop\\code\\dashboard\\Kohls_1\\" #ensure there is \\ at the end of the path
# folder where you put the main code
path_R="C:\\Users\\yuemeng1\\Desktop\\code\\dashboard\\R"

#is.staging=T  True is to staging DB and F is to production DB
#update=T if you want to delete all the info from this client, or it is a new client, put it as T

setwd(path_client)
final=fread("dsh_modelinput_data.csv")
datelkup=fread("dsh_input_lookup_date.csv")
varlkup=fread("dsh_input_lookup_var.csv",na.strings="")
dmalkup=fread("dsh_input_lookup_dma.csv")
setin=fread("dsh_input_setup_drilldown.csv",na.strings="")
homesetup=fread("dsh_input_setup_home.csv")
typetable=fread("dsh_input_type.csv")
md=fread("dsh_input_setup_market_date.csv")
datelkup$week=as.Date(datelkup$week,"%m/%d/%Y")
setup=fread("dsh_input_setup.csv")


#Please check if the date format makes sense after this step
final$week=as.Date(final$week)

if(setup$update ==1) update=T else update=F
if(setup$is.staging==1) is.staging=T else is.staging=F
if(setup$is.new.client==1) is.new.client=T else is.new.client=F

# DB server info
db.name="nviz"
port=3306
if (is.staging){
  db.server="127.0.0.1"
  username="root"
  password="bitnami"
  export_root="bitnami@ec2-54-175-246-49.compute-1.amazonaws.com:/home/rstudio/nviz/export/dashboard/"
}else{
  db.server="127.0.0.1"
  username="Zkdz408R6hll"
  password="XH3RoKdopf12L4BJbqXTtD2yESgwL$fGd(juW)ed"
  export_root="bitnami@ec2-52-2-65-22.compute-1.amazonaws.com:/home/rstudio/nviz/export/dashboard/"
}

conn <- dbConnect(MySQL(),user=username, password=password,dbname=db.name, host=db.server)

#########################
#Transform and uploading#
#########################

setwd(path_R)
source("adm_transform.R",local=F)
source("adm_update.R",local=F)
run=adm_update()
source("adm_export.R",local=F)






