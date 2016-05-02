get_name = function (remote_table) {
  ext_col=data.table(
    dbGetQuery(conn,paste("SELECT * from Information_schema.columns  where Table_name like '",remote_table,"'",sep="")))[["COLUMN_NAME"]]
  
  return(ext_col)
}


add_tag = function (label_table,label_name) { 
  #label_table should be whether modelinput_Data or drilldown label_name should be a string
  
  table_name=paste("dsh_label_",label_name,sep="")
  addtag <- tryCatch(
    {
      tryread=dbGetQuery(conn, paste("select * from ",table_name," limit 1",sep = ""))
    },
    error=function(e) {
      dbGetQuery(conn,paste("CREATE TABLE",table_name,
                            "(`label` VARCHAR(191) NOT NULL COLLATE 'utf8mb4_bin',`id` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`),UNIQUE INDEX `uni` (`label`)) COLLATE='utf8mb4_unicode_ci'ENGINE=InnoDB;"))
      print(paste("Note: Added Table ",table_name,sep=""))
      return(NA)
    },
    finally = {
      temp_new=unique(label_table[,label_name,with=F])
      # print(temp_new)
      setnames(temp_new,"label")
      temp_new[,label:=as.character(label)]
      temp_new=temp_new[!is.na(label)]
      temp_ext=data.table(dbGetQuery(conn,paste("select * from ",table_name,sep="")))
      temp=merge(temp_new,temp_ext,by=c("label"),all.x=T)
      temp=temp[is.na(id)]
      if (nrow(temp)!=0) {
        temp[,id:=NULL]
        dbWriteTable(conn,table_name,temp,append=T,row.names = F,header=F)
      }
    }
  )
  return(addtag)
}


lkup_table = function(table_name,lkup_var_list) { 
  #table_name is a data table name, lkup_var_list is a vector with colnames which need to be transformed into _id
  
  for(i in 1:length(lkup_var_list)) {
    sql_table_name = paste("dsh_label_",lkup_var_list[i],sep="")
    col_id = paste(lkup_var_list[i],"_id",sep="")
    lkup.data=data.table(dbGetQuery(conn,paste("select * from ",sql_table_name,sep="")))
    setnames(lkup.data,c("label","id"),c(lkup_var_list[i],col_id))
    temp=table_name[[lkup_var_list[i]]]
    temp=as.character(temp)
    table_name[,lkup_var_list[i]:=NULL]
    table_name=cbind(table_name,temp)
    setnames(table_name,"temp",lkup_var_list[i])
    table_name=merge(table_name,lkup.data,by=eval(lkup_var_list[i]),all.x=T)
    table_name[,c(lkup_var_list[i]):=NULL]
    
  }
  return(table_name)
}


add_col = function (col_check,remote_table) {
  col_add=col_check[!col_check %in% get_name(remote_table)]
  if (length(col_add)!=0) {
    for (i in 1:length(col_add)){
      if (any(grep("_id",col_add[i]))) {
        dbGetQuery(conn,paste("ALTER TABLE ",remote_table," ADD COLUMN ",col_add[i]," INT NULL DEFAULT NULL;",sep=""))
      } else if (any(grep("m_",col_add[i]))) {
        dbGetQuery(conn,paste("ALTER TABLE ",remote_table," ADD COLUMN ",col_add[i]," DOUBLE NULL DEFAULT NULL;",sep=""))
      } else {
        stop ("Warning: Check the column that need to be added on the remote.")
      }
      print(paste("Note: Added Column ",col_add[i]," In the table",remote_table,sep = ""))
    }
  }
}


upload_table = function (local_table,remote_table,ifupdate) {
  if(ifupdate) {
    # delete all current records from the client
    a=dbGetQuery(conn,paste("delete from ",remote_table," where client_id=",client_id,sep=""))
  } 
  dbWriteTable(conn,remote_table,local_table,append=T,row.names = F,header=F)
}


lkup_cell = function (vector_with_dlmt,remote_table) {
  result=unlist(strsplit(vector_with_dlmt,","))
  result=data.table(label=result)
  result=merge(result,data.table(dbGetQuery(conn,paste("select * from ",remote_table,sep=""))),by=c("label"),all.x=T)
  if(sum(is.na(result$id))!=0) {
    stop ("Note: please check your map_var in home_setup")
  }
  output = paste(result$id,collapse=",")
  return(output)
}
