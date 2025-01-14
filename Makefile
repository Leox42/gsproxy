
.PHONY: processor
.PHONY: proxy

processor:
				echo "Creating Processor's folders: $(NAME)"
				mkdir "processor.$(NAME)"
				mkdir "./processor.$(NAME)/input.$(NAME)"
				mkdir "./processor.$(NAME)/output.$(NAME)"
				mkdir "./processor.$(NAME)/sync.$(NAME)"
				mkdir "./processor.$(NAME)/sync.$(NAME)/processor"
				mkdir "./processor.$(NAME)/sync.$(NAME)/processes"
				mkdir "./processor.$(NAME)/tmp.$(NAME)"
				mkdir "./processor.$(NAME)/tmp.$(NAME)/envs"
				mkdir "./processor.$(NAME)/tmp.$(NAME)/run"

processor_delete:
				echo "Removing Processor: $(NAME)"
				rm -rf "processor.$(NAME)"

proxy:
				echo "Creating Proxy's folders: $(NAME)"
				mkdir "proxy.$(NAME)"
				mkdir "./proxy.$(NAME)/Archive.$(NAME)"
				mkdir "./proxy.$(NAME)/Data.$(NAME)"
				mkdir "./proxy.$(NAME)/messageLogFolder.$(NAME)"
				mkdir "./proxy.$(NAME)/syncFolder.$(NAME)"
				mkdir "./proxy.$(NAME)/Archive.$(NAME)/log_modules.$(NAME)"
				mkdir "./proxy.$(NAME)/Archive.$(NAME)/reports.$(NAME)"
				mkdir "./proxy.$(NAME)/Archive.$(NAME)/scheduledTask.$(NAME)"
				echo "ID,runID,Name,Target,Queue,Priority,MasterTableURL,MessageDetailTableURL,MessageURL,ProxyAuthKey,Processor,fileCrypting,runBlockChain,Status" > "./proxy.$(NAME)/Archive.$(NAME)/TASK_list_log.csv"
				echo "Proc_ID,Processor_name,Location,processorInputDir,processorSyncDir,processorOutputDir,Status,nRunningProcesses" > "./proxy.$(NAME)/Archive.$(NAME)/processor_list.csv"
				echo "dashID,Name,MasterTableURL,MessageDetailTable,MessageURL,proxyID" > "./proxy.$(NAME)/Archive.$(NAME)/GUI_configuration_file.csv"
				touch "./proxy.$(NAME)/Archive.$(NAME)//log_modules.$(NAME)/proc_out.txt"
				touch "./proxy.$(NAME)/Archive.$(NAME)//log_modules.$(NAME)/proxy_out.txt"
				touch "./proxy.$(NAME)/Archive.$(NAME)//log_modules.$(NAME)/proxy_out_logger.txt"
				touch "./proxy.$(NAME)/Archive.$(NAME)//log_modules.$(NAME)/sbcommlib_out_logger.txt"
				touch "./proxy.$(NAME)/Archive.$(NAME)//log_modules.$(NAME)/sched_out.txt"


associate:
				echo "Associating Processor $(PROC) to Proxy $(PROX)"
				echo $(PROCID),$(PROC),$(PROX),"./../../processor.$(PROC)/input.$(PROC)/","./../../processor.$(PROC)/sync.$(PROC)/","./../../processor.$(PROC)/output.$(PROC)/",Running,0 >> "./proxy.$(PROX)/Archive.$(PROX)/processor_list.csv"

proxy_populate:
				echo "Populating Proxy's folders $(NAME) with $(DATA_FOLD), $(ALG_FOLD)"
				cp "./$(DATA_FOLD)/DATAMART_list.csv" "./proxy.$(NAME)/Archive.$(NAME)"
				cp -r "./$(DATA_FOLD)/datamartDescription" "./proxy.$(NAME)/Archive.$(NAME)"
				cp -r "./$(DATA_FOLD)/" "./proxy.$(NAME)/Data.$(NAME)"
				cp "./$(ALG_FOLD)/ALGO_list.csv" "./proxy.$(NAME)/Archive.$(NAME)"

proxy_delete:
				echo "Removing: $(NAME)"
				rm -rf "proxy.$(NAME)"

run_proxy:
				python3 ./main.py compute -t $(PROX) -s "./proxy.$(PROX)/syncFolder.$(PROX)/" -a "./proxy.$(PROX)/Archive.$(PROX)/" -d "./proxy.$(PROX)/Data.$(PROX)/" -l "./proxy.$(PROX)/messageLogFolder.$(PROX)/" -g "./proxy.$(PROX)/Archive.$(PROX)/GUI_configuration_file.csv" -lf "./proxy.$(PROX)/Archive.$(PROX)/log_modules.$(PROX)/proxy_out_logger.txt" -c "./config.yaml"

run_processor:
				Rscript ./run_processor.R $(PROC) $(shell pwd)
