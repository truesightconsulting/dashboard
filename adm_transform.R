print("Now Transforming data.")

dma=c(colnames(final)[grep("market_",colnames(final))],colnames(final)[grep("dma",colnames(final))])
time_period=c("week")


dim=final[,c(dma,time_period),with=F]
numeric.part=final[,colnames(final)[!colnames(final) %in% c(dma,time_period)],with=F]
numeric.part=as.data.table(sapply(numeric.part,as.numeric))
final=cbind(dim,numeric.part)
final.process=melt(final,id.vars=c(dma,time_period),na.rm = T)
final.process=final.process[value!=0]
final.process=merge(final.process,datelkup,by=c("week"),all.x=T)
final.process=merge(final.process,varlkup,by=c("variable"),all.x=T)
final.process=final.process[!is.na(var)]
final.process=final.process[!is.na(d_1)]

# export=final.process[,c(time_period,dma,"value","variable"),with=F]
export.lkup=unique(final.process[,c("variable","export_1"),with=F])
final.process[,c("variable","export_1"):=NULL]

dcast_f=colnames(final.process)[!colnames(final.process) %in% c("metric","value")]
dcast_f2=as.formula(paste(paste(dcast_f,collapse="+"),"metric",sep="~"))

final.process=dcast.data.table(final.process,dcast_f2,sum,value.var=c("value"))
# final.process=merge(final.process,dmalkup,by=c(dma),all.x=T)

final.process=final.process[,c(dma,time_period,"type","var",colnames(final.process)[!colnames(final.process) %in% c(dma,time_period,"type","var")]),with=F]

setnames(export.lkup,c("variable","export_1"),c("var","label"))

#######################################
#generate and upload export.lkup.final#
#######################################

export.lkup.final=data.table(var=colnames(final))
export.lkup.final=merge(export.lkup.final,export.lkup,by=c("var"),all.x=T)
export.lkup.final[var=="dmanum",label:="DMA Number"]
export.lkup.final[var=="week",label:="Week"]
mk=data.table(t(md),keep.rownames=T)
mk=mk[grep("market_",mk$rn)]
mk$rn=gsub("var_","",mk$rn)
setnames(mk,c("var","label"))
setkey(export.lkup.final,var)
setkey(mk,var)
export.lkup.final[mk,':='(label=i.label)]
export.lkup.final=export.lkup.final[!is.na(label)]




##################
#update client id#
##################
print("Note: Now checking client set up.")
client_ext=data.table(dbGetQuery(conn,"select name, id from clients"))
client_current=current=data.table(name=client_name)
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
