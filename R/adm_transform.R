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
# final.process[,variable:=NULL]

export=final.process[,c("week","dmanum","value","variable"),with=F]
export.lkup=unique(final.process[,c("variable","export_1"),with=F])
final.process[,c("variable","export_1"):=NULL]

dcast_f=colnames(final.process)[!colnames(final.process) %in% c("metric","value")]
dcast_f2=as.formula(paste(paste(dcast_f,collapse="+"),"metric",sep="~"))

final.process=dcast.data.table(final.process,dcast_f2,sum,value.var=c("value"))
final.process=merge(final.process,dmalkup,by=c("dmanum"),all.x=T)

final.process=final.process[,c("dmanum","week","type","var",colnames(final.process)[!colnames(final.process) %in% c("dmanum","week","type","var")]),with=F]
# final.process[,client_id:=client_id]

# export=merge(export,dmalkup,by=c("dmanum"),all.x=T)
# export[,dmanum:=NULL]
export.final=dcast.data.table(export,dmanum+week~variable,sum,value.var=c("value"))
export.final=merge(export.final,dmalkup,by=c("dmanum"),all.x=T)
# export.final[,dmanum:=NULL]
setnames(export.lkup,c("variable","export_1"),c("var","label"))

#######################################
#generate and upload export.lkup.final#
#######################################

export.lkup.final=data.table(var=colnames(export.final))
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
