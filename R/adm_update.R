#Read current tables and vars from DB
ex.sample.data=data.table(dbGetQuery(conn,paste("SELECT * from Information_schema.columns  where Table_name like 'dsh_modelinput_data'")))
ex.sample.setup=data.table(dbGetQuery(conn,paste("SELECT * from Information_schema.columns  where Table_name like 'dsh_modelinput_setup'")))
old_colname_data=ex.sample.data[["COLUMN_NAME"]]
old_colname_data=old_colname_data[grep("_",old_colname_data)]
old_colname_setup=ex.sample.setup[["COLUMN_NAME"]]
old_colname_setup=old_colname_setup[grep("var_",old_colname_setup)]


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
      dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_setup ADD COLUMN ",temp_var_setup," INT NULL DEFAULT NULL;",sep=""))
      
      #add tables to database (only one table)
      dbGetQuery(conn,paste("CREATE TABLE",
                            paste("dsh_label_",temp_label1,sep=""),
                            "(`label` VARCHAR(191) NOT NULL COLLATE 'utf8mb4_bin',`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`),UNIQUE INDEX `uni` (`label`)) COLLATE='utf8mb4_unicode_ci'ENGINE=InnoDB;"))
      
    } else {
      #for adding filter variables
      temp_var_data=col_to_add_data[i]
      temp_var_setup=col_to_add_setup[i]
      temp_label1=gsub("_id","",col_to_add_setup[i])
      temp_label2=gsub("_id","",col_to_add_data[i])
      
      #add columns to database
      dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_data ADD COLUMN ",temp_var_data," INT NULL DEFAULT NULL;",sep=""))
      dbGetQuery(conn,paste("ALTER TABLE dsh_modelinput_setup ADD COLUMN ",temp_var_setup," INT NULL DEFAULT NULL;",sep=""))
      
      #add tables to database (two tables to add)
      dbGetQuery(conn,paste("CREATE TABLE",
                            paste("dsh_label_",temp_label1,sep=""),
                            "(`label` VARCHAR(191) NOT NULL COLLATE 'utf8mb4_bin',`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`),UNIQUE INDEX `uni` (`label`)) COLLATE='utf8mb4_unicode_ci'ENGINE=InnoDB;"))
      dbGetQuery(conn,paste("CREATE TABLE",
                            paste("dsh_label_",temp_label2,sep=""),
                            "(`label` VARCHAR(191) NOT NULL COLLATE 'utf8mb4_bin',`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`),UNIQUE INDEX `uni` (`label`)) COLLATE='utf8mb4_unicode_ci'ENGINE=InnoDB;"))
      
      
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
# final.process[,client_id:=client_id]


#Upload updated database
setnames(final.process,c("dmanum","week"),c("dma","date"))
if(update) {
  # delete all current records from the client
  dbGetQuery(conn,paste("delete from dsh_modelinput_data where client_id=",client_id,sep=""))
  dbWriteTable(conn,"dsh_modelinput_data",final.process,append=T,row.names = F,header=F)
} else {
    #will just append the ex.new.data in the existing dsh_modelinput_data
    dbWriteTable(conn,"dsh_modelinput_data",final.process,append=T,row.names = F,header=F)
}
