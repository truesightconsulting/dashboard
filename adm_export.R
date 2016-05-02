######################
#zip the export files#
######################
setwd(path_client)

write.csv(final,"output_export_data.csv",row.names=F)
write.csv(export.lkup.final,"output_export_lookup.csv",row.names=F)
temp=paste(path_system,"7za a -tzip raw_data.zip ",path_client,"output_export*.*",sep="")
shell(temp)

upload_cmd=paste(path_system,"pscp -i ",path_key," ",path_client,"raw_data.zip ",export_root,client_id,sep="")
shell(upload_cmd)
file.remove("output_export_data.csv")
file.remove("output_export_lookup.csv")
file.remove("raw_data.zip")

if(!is.staging) {
  zip_input=paste(path_system,"7za a -tzip dsh_modelinput_data.zip ",path_client,"dsh_modelinput_data.csv",sep="")
  shell(zip_input)
  upload_input=paste(path_system,"pscp -i ",path_key," ",path_client,"dsh_modelinput_data.zip ",export_root,client_id,sep="")
  shell(upload_input)
  file.remove("dsh_modelinput_data.zip")
  # file.remove("raw_data.zip")
}