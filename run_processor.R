library(GSBox) 
library(jsonlite) 

args <- commandArgs(trailingOnly = TRUE) 




nome = args[1]
path = paste(args[2],"/processor.",nome, sep="")


objS <- bckGndProcessor(input_folder.dir = paste(path,"/input.",nome,"/", sep=""),
                        output_folder.dir = paste(path,"/output.",nome,"/", sep=""), #
                        tmp_folder.dir = paste(path,"/tmp.",nome,"", sep=""), 
                        sync_folder.dir = paste(path,"/sync.",nome,"/", 
                        sep=""), override.repeated.tUID = TRUE
)

print(paste("Starting Processor", nome))
objS$start()
