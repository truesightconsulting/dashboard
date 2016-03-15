adm_update = function(...) {
  ##################
  #update client id#
  ##################
  print("Note: Now checking client set up.")
  client_ext=data.table(dbGetQuery(conn,"select name, id from clients"))
  client_current=current=data.table(name=homesetup$client_name)
  is.client.ext = client_current %in% client_ext$name
  if(is.new.client ==T & is.client.ext ==T) {
    stop ("Note: The new client name is already exist.")
  } else if (is.new.client ==F & is.client.ext ==F){
    stop ("Note: The client is not in the database.")
  } else if (is.new.client ==T & is.client.ext ==F) {
    dbWriteTable(conn,"clients",client_current,append=T,row.names = F,header=F)
    client_ext=data.table(dbGetQuery(conn,"select name, id from clients"))
    client_id = client_ext[name==client_current,]$id
  } else if (is.new.client ==F & is.client.ext ==T){
    client_id = client_ext[name==client_current,]$id
  }
  
  #############################
  #update type id and yaxis id#
  #############################
  
  print("Note: Now Generating tag tables.")
  type_ext=data.table(dbGetQuery(conn,"select * from dsh_label_type"))
  yaxis_ext=data.table(dbGetQuery(conn,"select * from dsh_label_type_yaxis"))
  
  if(sum(!typetable$type_name %in% type_ext$label)!=0) {
    type.new=data.table(label=typetable$type_name[!typetable$type_name %in% type_ext$label])
    dbWriteTable(conn,"dsh_label_type",type.new,append=T,row.names = F,header=F)
    
  }
  if(sum(!typetable$type_yaxis %in% yaxis_ext$label)!=0) {
    yaxis.new=data.table(label=typetable$type_yaxis[!typetable$type_yaxis %in% yaxis_ext$label])
    dbWriteTable(conn,"dsh_label_type_yaxis",yaxis.new,append=T,row.names = F,header=F)
    
  }
  type_ext=data.table(dbGetQuery(conn,"select * from dsh_label_type"))
  yaxis_ext=data.table(dbGetQuery(conn,"select * from dsh_label_type_yaxis"))
  setnames(type_ext,c("label","id"),c("type_name","type_id"))
  setnames(yaxis_ext,c("label","id"),c("type_yaxis","type_yaxis_id"))
  typetable_upload=merge(typetable,type_ext,by=c("type_name"),all.x=T)
  typetable_upload=merge(typetable_upload,yaxis_ext,by=c("type_yaxis"),all.x=T)
  typetable_upload[,c("type_name","type_yaxis"):=NULL]
  typetable_upload[,client_id:=client_id]
  
  dbGetQuery(conn,paste("delete from dsh_modelinput_type where client_id=",client_id,sep=""))
  dbWriteTable(conn,"dsh_modelinput_type",typetable_upload,append=T,row.names = F,header=F)
  
  
  ############################
  #update tag #update main db#
  ############################
  
  #Read current tables and vars from DB
  old_colname_data=data.table(
    dbGetQuery(conn,paste("SELECT * from Information_schema.columns  where Table_name like 'dsh_modelinput_data'")))[["COLUMN_NAME"]]
  old_colname_data=old_colname_data[grep("_",old_colname_data)]
  old_colname_setup=data.table(
    dbGetQuery(conn,paste("SELECT * from Information_schema.columns  where Table_name like 'dsh_modelinput_drilldown_setup'")))[["COLUMN_NAME"]]
  old_colname_setup=old_colname_setup[grep("var_",old_colname_setup)]
  
  setin=cbind(setin,md)
  #Create list for vars and dbs need to be added
  new_colnames_setup=colnames(setin)[grep("var_",colnames(setin))]
  new_colnames_setup=paste(new_colnames_setup,rep("_id",length(new_colnames_setup)),sep="")
  col_to_add_setup=new_colnames_setup[!new_colnames_setup%in%old_colname_setup]
  
  #Add columns and Create necessary tables in DB
  if(length(col_to_add_setup)!=0) {
    col_to_add_data=gsub("var_","",col_to_add_setup)
    for(i in 1:length(col_to_add_data)) {
      if (any(grep("m_",col_to_add_data[i]))) {
        #delete _id from metric variable
        temp_var_data=gsub("_id","",col_to_add_data[i])
        temp_var_setup=col_to_add_setup[i]
        temp_label1=gsub("_id","",col_to_add_setup[i])
        
        #add columns to database
        dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_data ADD COLUMN ",temp_var_data," DOUBLE NULL DEFAULT NULL;",sep=""))
        dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_drilldown_setup ADD COLUMN ",temp_var_setup," INT NULL DEFAULT NULL;",sep=""))
        # dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_dim_f ADD COLUMN ",temp_var_data," DOUBLE NULL DEFAULT NULL;",sep=""))
        
        #add tables to database (only one table)
        dbGetQuery(conn,paste("CREATE TABLE",
                              paste("dsh_label_",temp_label1,sep=""),
                              "(`label` VARCHAR(191) NOT NULL COLLATE 'utf8mb4_bin',`id` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`),UNIQUE INDEX `uni` (`label`)) COLLATE='utf8mb4_unicode_ci'ENGINE=InnoDB;"))
        
      } else {
        #for adding filter variables
        temp_var_data=col_to_add_data[i]
        temp_var_setup=col_to_add_setup[i]
        temp_label1=gsub("_id","",col_to_add_setup[i])
        temp_label2=gsub("_id","",col_to_add_data[i])
        
        #add columns to database
        dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_data ADD COLUMN ",temp_var_data," INT NULL DEFAULT NULL;",sep=""))
        dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_drilldown_setup ADD COLUMN ",temp_var_setup," INT NULL DEFAULT NULL;",sep=""))
        dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_dim_f ADD COLUMN ",temp_var_data," INT NULL DEFAULT NULL;",sep=""))

        #add tables to database (two tables to add)
        dbGetQuery(conn,paste("CREATE TABLE",
                              paste("dsh_label_",temp_label1,sep=""),
                              "(`label` VARCHAR(191) NOT NULL COLLATE 'utf8mb4_bin',`id` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`),UNIQUE INDEX `uni` (`label`)) COLLATE='utf8mb4_unicode_ci'ENGINE=InnoDB;"))
        dbGetQuery(conn,paste("CREATE TABLE",
                              paste("dsh_label_",temp_label2,sep=""),
                              "(`label` VARCHAR(191) NOT NULL COLLATE 'utf8mb4_bin',`id` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`),UNIQUE INDEX `uni` (`label`)) COLLATE='utf8mb4_unicode_ci'ENGINE=InnoDB;"))
        
        
      }
    }
  }
  
  
  #Convert filters into filter_ids
  lkup.data=list()
  label_data=colnames(final.process)[!colnames(final.process) %in% c("dmanum","week","type","client_id",grep("m_",colnames(final.process),value=T))]
  
  for (i in 1:length(label_data)) {
    #update label-id files on the DB
    temp_new=unique(final.process[,label_data[i],with=F])
    setnames(temp_new,"label")
    temp_new[,label:=as.character(label)]
    temp_new=temp_new[!is.na(label)]
    temp_ext=data.table(dbGetQuery(conn,paste("select * from ",paste("dsh_label_",label_data[i],sep=""),sep="")))
    temp=merge(temp_new,temp_ext,by=c("label"),all.x=T)
    temp=temp[is.na(id)]
    if (nrow(temp)!=0) {
      temp[,id:=NULL]
      dbWriteTable(conn,paste("dsh_label_",label_data[i],sep=""),temp,append=T,row.names = F,header=F)
    }
    
    #replace the filters with filter_ids
    lkup.data[[i]]=data.table(dbGetQuery(conn,paste("select * from ",paste("dsh_label_",label_data[i],sep=""),sep="")))
    setnames(lkup.data[[i]],c("label","id"),c(label_data[i],paste(label_data[i],"_id",sep="")))
    temp=final.process[[label_data[i]]]
    temp=as.character(temp)
    final.process[,label_data[i]:=NULL]
    final.process=cbind(final.process,temp)
    setnames(final.process,"temp",label_data[i])
    final.process=merge(final.process,lkup.data[[i]],by=eval(label_data[i]),all.x=T)
    # final.process[,label_data[i]:=as.character(label_data[i])]
    final.process[,c(label_data[i]):=NULL]
  }
  final.process=final.process[order(var_id,week)]
  final.process[,client_id:=client_id]
  setnames(final.process,c("dmanum","week"),c("dma","date"))
  
  
  
  
  #update main db
  print("Note: Now uploading Database.")
  if(update) {
    # delete all current records from the client
    dbGetQuery(conn,paste("delete from dsh_modelinput_data where client_id=",client_id,sep=""))
  } 
  
  dbWriteTable(conn,"dsh_modelinput_data",final.process,append=T,row.names = F,header=F)
  
  ########################
  #update home setup page#
  ########################
  print("Note: Now uploading home_setup.")
  setin[setin==""]=NA
  setin = setin[,!sapply(setin, function (k) all(is.na(k))),with=F]
  setuplabel=list()
  lkupsetuplist=colnames(setin)[grep("var_",colnames(setin))]
  label2=paste(rep("dsh_label_",length(lkupsetuplist)),lkupsetuplist,sep="")
  
  for(i in 1:length(lkupsetuplist)) {
    setuplabel[[i]]=data.table(dbGetQuery(conn,paste("select * from ",label2[i],sep="")))
    if(sum(!unique(setin[[c(lkupsetuplist[i])]])[!is.na(unique(setin[[c(lkupsetuplist[i])]]))] %in% setuplabel[[i]]$label)!=0) {
      temp=data.table(
        label=unique(setin[[c(lkupsetuplist[i])]])[!is.na(unique(setin[[c(lkupsetuplist[i])]]))][!unique(setin[[c(lkupsetuplist[i])]])[!is.na(unique(setin[[c(lkupsetuplist[i])]]))] %in% setuplabel[[i]]$label])
      dbWriteTable(conn,label2[i],temp,append=T,row.names = F,header=F)
      setuplabel[[i]]=data.table(dbGetQuery(conn,paste("select * from ",label2[i],sep="")))
    }
    setnames(setuplabel[[i]],c("label","id"),c(lkupsetuplist[i],paste(lkupsetuplist[i],"id",sep="_")))
    setin=merge(setin,setuplabel[[i]],by=c(lkupsetuplist[i]),all.x=T)
    setin[,c(lkupsetuplist[i]):=NULL]
  }
  market_check = unique(setin[,c(colnames(setin)[grep("market_",colnames(setin))]),with=F])
  if(nrow(market_check)!=1) {
    stop ("Note: Market level Error")
  }
#   
  map.var=unlist(strsplit(homesetup$map_var,","))
  map.var=data.table(label=map.var)
  map.var=merge(map.var,data.table(dbGetQuery(conn,"select * from dsh_label_var")),by=c("label"),all.x=T)
  if(sum(is.na(map.var$id))!=0) {
    stop ("Note: please check your map_var in home_setup")
  }
  homesetup[,map_var:=paste(map.var$id,collapse=",")]
  
  
  homesetup$date_start=as.Date(homesetup$date_start,"%m/%d/%Y")
  homesetup$date_end=as.Date(homesetup$date_end,"%m/%d/%Y")
  homesetup[,c("update","is.staging","is.new.client","client_name"):=NULL]
  homesetup=cbind(homesetup,market_check)
  homesetup[,client_id:=client_id]
  date_minmax = dbGetQuery(conn,paste("select min(date) as date_min, max(date) as date_max from dsh_modelinput_data where client_id =",client_id,sep=""))
  homesetup=cbind(homesetup,date_minmax)
  
  dbGetQuery(conn,paste("delete from dsh_modelinput_home_setup where client_id=",client_id,sep=""))
  dbWriteTable(conn,"dsh_modelinput_home_setup",homesetup,append=T,row.names = F,header=F)
  
  
  #########################
  #update modelinput pages#
  #########################
  
  print("Note: Now Generating Dimension pages.")
  dim_table=c("d","f","map","market","market_drilldown")
  dim_table = paste(rep("dsh_modelinput_dim_",length(dim_table)),dim_table,sep="")
  
  for(i in 1:length(dim_table)) {
    temp_name=data.table(dbGetQuery(conn,paste("SELECT * from Information_schema.columns
                                               where Table_name like '",dim_table[i],"'",sep="")))[["COLUMN_NAME"]]
    temp_name=temp_name[!temp_name %in% c("id")]
    dbGetQuery(conn,paste("delete from ",dim_table[i]," where client_id=",client_id,sep=""))
    dbGetQuery(conn,
               paste("insert into ",dim_table[i]," (",paste(temp_name,collapse=","),") ",
                     "select distinct ",paste(temp_name,collapse=",")," from dsh_modelinput_data where client_id=",client_id,
                     sep="")
    )
    
  }
  
  #############################
  #update drilldown setup page#
  #############################
  
  print("Note: Now Uploading Drilldown Setup.")
  var.id=data.table(dbGetQuery(conn,"select * from dsh_label_var"))
  setnames(var.id,c("label","id"),c("var","var_id"))
  setin=merge(setin,var.id,by=c("var"),all.x=T)
  if(sum(is.na(setin$var_id))!=1) {
    stop ("Please check your input_setup file. There is an var_name error.")
  }
  setin[,var:=NULL]
  setin[,client_id:=client_id]
  date_setin=homesetup[,c("date_start","date_end","date_min","date_max"),with=F]
  setin=cbind(setin,date_setin)
  dbGetQuery(conn,paste("delete from dsh_modelinput_drilldown_setup where client_id=",client_id,sep=""))
  dbWriteTable(conn,"dsh_modelinput_drilldown_setup",setin,append=T,row.names = F,header=F)

  
  #####################
  #update export files#
  #####################
  export.final[,client_id:=client_id]
  export.lkup.final[,client_id:=client_id]
  old_export_name=data.table(
    dbGetQuery(conn,paste("SELECT * from Information_schema.columns  where Table_name like 'dsh_modelinput_data_export'")))[["COLUMN_NAME"]]
  new_export_name=colnames(export.final)
  col_to_add_export=new_export_name[!new_export_name%in%old_export_name]
  
  if(length(col_to_add_export)!=0) {
    for(i in 1:length(col_to_add_export)) {
      dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_data_export ADD COLUMN ",col_to_add_export[i]," DOUBLE NULL DEFAULT NULL;",sep=""))
    }
  }
  
  print("Note: Now uploading Raw Database.")
  if(update) {
    # delete all current records from the client
    dbGetQuery(conn,paste("delete from dsh_modelinput_data_export where client_id=",client_id,sep=""))
    dbGetQuery(conn,paste("delete from dsh_modelinput_export_lkup where client_id=",client_id,sep=""))
  } 
  
  dbWriteTable(conn,"dsh_modelinput_data_export",export.final,append=T,row.names = F,header=F)
  dbWriteTable(conn,"dsh_modelinput_export_lkup",export.lkup.final,append=T,row.names = F,header=F)
  print("Note: Done.")
  
}