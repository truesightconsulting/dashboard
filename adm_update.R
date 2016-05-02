adm_update = function(...) {

  drilldown=cbind(setin,md)
  drilldown[drilldown==""]=NA
  drilldown=drilldown[,colSums(is.na(drilldown))<nrow(drilldown),with=F]
  
  setnames(typetable,c("type","type_name"),c("type_num_temp","type"))

  ###################
  #update tag tables#
  ###################
  #var_m,var_d,var_f,var_market is from drilldown
  #d_,f_,market_,tab_,var is from data (final.process)
  #type is from typetable
  print("Note: Now Updating Label Pages.")
  
  tag_from_drill = colnames(drilldown)[grep("var_",colnames(drilldown))]
  for(i in 1:length(tag_from_drill)) {
    add_tag(drilldown,tag_from_drill[i])
  }
  
  add_tag(typetable,"type")
  add_tag(typetable,"type_yaxis")
  
  tag_from_data=colnames(final.process)[!colnames(final.process) %in% c("dmanum","week","type",grep("m_",colnames(final.process),value=T))]
  for(i in 1:length(tag_from_data)) {
    add_tag(final.process,tag_from_data[i])
  }

  ################################################
  #transforming modelinput tables's name into _id#
  ################################################
  print("Note: Now Looking up variables.")
  
  data_lkup=tag_from_data
  drill_lkup=colnames(drilldown)[!colnames(drilldown) %in% c("type")]
  type_lkup=c("type","type_yaxis")
  
  final.process=lkup_table(final.process,data_lkup)
  drilldown=lkup_table(drilldown,drill_lkup)
  typetable=lkup_table(typetable,type_lkup)

  ##########################################################
  #uploading modelinput tables (not include modelinput_dim)#
  ##########################################################
  
  #dsh_modelinput_data
  print("Note: Now Uploading dsh_modelinput_data")
  
  final.process=final.process[order(var_id,week)]
  final.process[,client_id:=client_id]
  setnames(final.process,c("dmanum","week"),c("dma","date"))
  
  add_col(colnames(final.process),"dsh_modelinput_data")
  upload_table(final.process,"dsh_modelinput_data",update)
  
  
  #dsh_modelinput_type
  print("Note: Now Uploading dsh_modelinput_type")
  
  setnames(typetable,"type_num_temp","type")
  typetable[,client_id:=client_id]
  
  add_col(colnames(typetable),"dsh_modelinput_type")
  upload_table(typetable,"dsh_modelinput_type",T)
  
  
  #dsh_modelinput_home_setup (still need some cbind and transform work)
  print("Note: Now Uploading dsh_modelinput_home_setup")
  
  mapvar=lkup_cell(homesetup$map_var,"dsh_label_var")
  type1=lkup_cell(homesetup$type1_filter,"dsh_label_var")
  
  homesetup[,":=" (map_var = mapvar,type1_filter=type1)]
  homesetup$date_start=as.Date(homesetup$date_start,"%m/%d/%Y")
  homesetup$date_end=as.Date(homesetup$date_end,"%m/%d/%Y")
  
  market_check = unique(drilldown[,c(colnames(drilldown)[grep("market_",colnames(drilldown))]),with=F])
  if(nrow(market_check)!=1) {
    stop ("Note: Market level Error")
  }
  homesetup=cbind(homesetup,market_check)
  homesetup[,client_id:=client_id]
  date_minmax = dbGetQuery(conn,paste("select min(date) as date_min, max(date) as date_max from dsh_modelinput_data where client_id =",client_id,sep=""))
  homesetup=cbind(homesetup,date_minmax)
  
  add_col(colnames(homesetup),"dsh_modelinput_home_setup")
  upload_table(homesetup,"dsh_modelinput_home_setup",T)
  
  
  #dsh_modelinput_drilldown_Setup
  print("Note: Now Uploading dsh_modelinput_drilldown_Setup")
  
  drilldown[,client_id:=client_id]
  date_drill=homesetup[,c("date_start","date_end","date_min","date_max"),with=F]
  drilldown=cbind(drilldown,date_drill)
  
  add_col(colnames(drilldown),"dsh_modelinput_drilldown_setup")
  upload_table(drilldown,"dsh_modelinput_drilldown_setup",T)


  #######################################
  #generating dsh_modelinput_dim_ tables#
  #######################################
  print("Note: Now Setting dsh_modelinput_dim_ tables")
  
  #Setup Table names and correspoding col names
  dim_table = c("d","f","map","market","market_drilldown","tab")
  exist_col = get_name("dsh_modelinput_data")
  
  var_dim_table = list()
  var_dim_table$d = exist_col[exist_col %in% c(grep("tab_",exist_col,value=T),grep("d_",exist_col,value=T))]
  var_dim_table$f = exist_col[exist_col %in% c(grep("tab_",exist_col,value=T),grep("f_",exist_col,value=T))]
  var_dim_table$map = "var_id"
  var_dim_table$market = exist_col[exist_col %in% c(grep("market_",exist_col,value=T))]
  var_dim_table$market_drilldown = exist_col[exist_col %in% c(grep("market_",exist_col,value=T))]
  var_dim_table$tab = exist_col[exist_col %in% c(grep("tab_",exist_col,value=T))]
  
  print("Note: Now Uploading dsh_modelinput_dim_ tables")
  
  for (i in 1:length(dim_table)) {
    table_r=paste("dsh_modelinput_dim_",dim_table[i],sep="")
    add_col(var_dim_table[[dim_table[i]]],table_r)
    
    temp_name=data.table(dbGetQuery(conn,paste("SELECT * from Information_schema.columns
                                               where Table_name like '",table_r,"'",sep="")))[["COLUMN_NAME"]]
    temp_name=temp_name[!temp_name %in% c("id")]
    dbGetQuery(conn,paste("delete from ",table_r," where client_id=",client_id,sep=""))
    dbGetQuery(conn,
               paste("insert into ",table_r," (",paste(temp_name,collapse=","),") ",
                     "select distinct ",paste(temp_name,collapse=",")," from dsh_modelinput_data where client_id=",client_id,
                     sep="")
               )
  }
  # return(list(client_id=client_id))
}