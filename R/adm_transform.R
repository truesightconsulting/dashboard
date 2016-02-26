print("Now Transforming data.")

dim=final[,c("dmanum","week"),with=F]
numeric.part=final[,colnames(final)[!colnames(final) %in% c("dmanum","week")],with=F]
numeric.part=as.data.table(sapply(numeric.part,as.numeric))
final=cbind(dim,numeric.part)
final.process=melt(final,id.vars=c("dmanum","week"),na.rm = T)
final.process=final.process[value!=0]
final.process=merge(final.process,datelkup,by=c("week"),all.x=T)
final.process=merge(final.process,varlkup,by=c("variable"),all.x=T)
final.process=final.process[!is.na(var)]
final.process=final.process[!is.na(d_1)]
final.process[,variable:=NULL]
dcast_f=colnames(final.process)[!colnames(final.process) %in% c("metric","value")]
dcast_f2=as.formula(paste(paste(dcast_f,collapse="+"),"metric",sep="~"))

final.process=dcast.data.table(final.process,dcast_f2,sum,value.var=c("value"))
final.process=merge(final.process,dmalkup,by=c("dmanum"),all.x=T)

final.process=final.process[,c("dmanum","week","type","var",colnames(final.process)[!colnames(final.process) %in% c("dmanum","week","type","var")]),with=F]
# final.process[,client_id:=client_id]
