print("Now Transforming data.")

final.process=melt.data.table(final,id.vars=c("dmanum","week"),na.rm = T)

final.process=merge(final.process,datelkup,by=c("week"),all.x=T)
final.process=merge(final.process,varlkup,by=c("variable"),all.x=T)
final.process=final.process[!is.na(var)]
final.process=final.process[!is.na(f_1)]
final.process[,variable:=NULL]
dcast_f=colnames(final.process)[!colnames(final.process) %in% c("metric","value")]
dcast_f2=as.formula(paste(paste(dcast_f,collapse="+"),"metric",sep="~"))

final.process=dcast.data.table(final.process,dcast_f2,sum,value.var=c("value"))
final.process=merge(final.process,dmalkup,by=c("dmanum"),all.x=T)

final.process=final.process[,c("dmanum","week","type","var",colnames(final.process)[!colnames(final.process) %in% c("dmanum","week","type","var")]),with=F]
final.process[,client_id:=client_id]
