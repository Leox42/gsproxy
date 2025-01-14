import warnings
from pathlib import Path
from datetime import datetime
import shutil
import zipfile
from os import listdir

from Proxy.src.authentication import *
from Proxy.src.dataretrieval import *
from Proxy.src.blockchain_handler import BlockChainHandler
import yaml
import logging
from urllib.error import HTTPError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

with open('./config.yaml', "r") as f:
    yaml_config = yaml.safe_load(f)

def setup_logger(log_file):
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

class Proxy:
    def __init__(self, target, stopfolder, guilist, archiveInfoPath, dataStoragePath, messageLogFolder, logger_file):
        self.target = target
        self.stopFolder = stopfolder
        self.log_memory = {}
        self.GUI_list = pd.read_csv(guilist, dtype=str).fillna("")
        self.archiveInfoPath = archiveInfoPath
        self.archiveReports = archiveInfoPath + 'reports'
        self.dataStoragePath = dataStoragePath
        self.messageLogFolder = messageLogFolder
        self.logger_file = logger_file
        setup_logger(self.logger_file)
        with open('./config.yaml', "r") as f:
            yaml_config = yaml.safe_load(f)
        self.authhost = yaml_config['PROXY']['authhost']
        self.proxy_name = target
        self.authenticated_usr_token = []
        self.authenticator = Authenticator(send_status_message_func=send_status_message,
                                           messageLogFolder=self.messageLogFolder,
                                           GUI_list=self.GUI_list,
                                           authhost=self.authhost,
                                           auth_logger=logger,
                                           )
        self.blockchain_handler = BlockChainHandler(bc_logger=logger, config_file_path='./config.yaml')

    def start(self):

        logger.info('--- Starting Proxy ---')
        print('--- Starting Proxy ---')
        self.authenticator.start_timer()
        logger.info('# Authenticator tool started')
        logger.info('##### Proxy Running #####')

        while True:
            if len(self.GUI_list) > 0:
                for index, gui in self.GUI_list.iterrows():
                    self.new_task_management(gui)
            else:
                logger.warning("Proxy is not associated with any GUI. Update GUI configuration file please!")

            try:
                self.new_output_management()
            except HTTPError as e:
                logger.error(f'Error in new_output_management with error {e}!')

            self.log_management(self.log_memory, self.archiveInfoPath)

    ##### PROXY input #####
    def new_task_list_read(self, url_MMT, url_MMD, url_MSG, proxyAuthKey, TASK_list_log):

        try:
            task_list_log = pd.read_csv(TASK_list_log)
        except FileNotFoundError:
            logger.error(f"File {TASK_list_log} not found!")
            return None
        except PermissionError:
            logger.error(f"Insufficient permission to read {TASK_list_log}!")
            return None
        except IsADirectoryError:
            logger.error(f"{TASK_list_log} is a directory!")
            return None

        task_list = pull_new_tasks(url_MMT, url_MMD, url_MSG, proxyAuthKey)

        if task_list is not None:

            task_list['ID'] = task_list['ID'].astype(str)
            task_list['runID'] = task_list['runID'].astype(str)
            task_list_log['ID'] = task_list_log['ID'].astype(str)
            task_list_log['runID'] = task_list_log['runID'].astype(str)

            task_list_log = task_list.merge(task_list_log, on=["ID", "runID"], how="left", suffixes=("", "_"))[
                list(task_list_log)]
            new_tasks = task_list_log[(task_list_log["Status"].isna() == True)].sort_values("Priority").reset_index(
                drop=True)

            new_tasks = new_tasks[new_tasks['Status'] != 'Verified']
            nNewTasks = len(new_tasks)
            if nNewTasks > 0:
                logger.info(f"There are {str(nNewTasks)} new tasks!")

            return new_tasks
        else:
            logger.error('Error reading new tasks, failed pulling new tasks.')
            print('Error reading new tasks, failed pulling new tasks.')
            return None

    def prepare_for_scheduling(self, job, xml, scheduling):
        MMT_xml_job = """
        <xml>
            <job ID='##ID##'>
                <runID>##runID##</runID>
                <name>##Name##</name>
                <queue>Processor</queue>
                <targetProxy>##Target##</targetProxy>
                <priority>##Priority##</priority>
                <source type = 'URI'>nulla per ora</source>
            </job>
        </xml>
        """

        MMT_xml_job = MMT_xml_job.replace('##ID##', job['ID']).replace('##Name##', job['Name']).replace('##Priority##',
                                                                                                        job['Priority'])
        text_file = open(self.archiveInfoPath + "scheduledTask/" + str(job['ID']) + "_MMT.xml", "w")
        text_file.write(MMT_xml_job)
        text_file.close()

        MMD_xml_job = re.sub('<scheduling(?s)(.*)scheduling>',
                             "<scheduling runNow='yes'><months></months><days></days><hours></hours><mins></mins></scheduling>",
                             xml.decode())
        text_file = open(self.archiveInfoPath + "scheduledTask/" + str(job['ID']) + "_MDT.xml", "w")
        text_file.write(MMD_xml_job)
        text_file.close()

        job = pd.concat([job, pd.Series(
            [scheduling['months'], scheduling['days'], scheduling['hours'],
             scheduling['mins']], index=['Month', 'Day', 'Hour', 'Min'])])
        return self.record2scheduler(job)

    def record2scheduler(self, job):
        df = pd.read_csv(self.archiveInfoPath + 'TASK_list_scheduled.csv')
        result = pd.concat([df, job.to_frame().T], ignore_index=True)
        result.to_csv(self.archiveInfoPath + 'TASK_list_scheduled.csv', index=False)
        return 'Scheduled'

    def token_preparation(self, jobID, runID, usrToken, datamart, algorithm, extra, query, processorInputDir, url_MSG):

        logger.info(f'Preparing token for job {jobID}, runID {runID}. Datamart:{datamart}, Algorithm:{algorithm}')

        authhost = 'IP'
        datamartList = auth_query_items_by_token(authhost, usrToken, 'datamart')
        algoList = auth_query_items_by_token(authhost, usrToken, 'algorithm')
        descriptor_xml = Path(self.archiveInfoPath + 'descriptor_template.xml').read_text()

        datmartCheck = datamartList['Tag'].isin([datamart]).sum()
        algoCheck = algoList['Tag'].isin([algorithm]).sum()
        if (datmartCheck == 1) & (algoCheck == 1):
            permCheck = True
        elif datmartCheck + algoCheck < 2:
            permCheck = False

            raise ValueError('Unresolved names - multiple resources with the same name')

        if permCheck:
            datamartFileName = datamartList[datamartList["Tag"] == datamart]["Source"].iloc[0]
            if datamartFileName.endswith('.zip'):
                datamartFileNameStandard = "output.zip"
            else:
                datamartFileNameStandard = "data_" + jobID + '_' + runID + ".csv"
            algoDockerImage = algoList[algoList["Tag"] == algorithm]["Source"].iloc[0]

            date_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            descriptor_xml = descriptor_xml.replace("###ID###", jobID + '_' + runID)
            descriptor_xml = descriptor_xml.replace("###runUID###", runID)
            descriptor_xml = descriptor_xml.replace("###creationDateTime###", date_time)
            descriptor_xml = descriptor_xml.replace("###dataSourceFileName###", datamartFileNameStandard)
            descriptor_xml = descriptor_xml.replace("###dataSourceAlias###", datamart)
            descriptor_xml = descriptor_xml.replace("###scriptAlias###", algoDockerImage)
            descriptor_xml = descriptor_xml.replace("###dockerImageName###", algoDockerImage)

            tokenName = "token_" + jobID + "_" + runID
            tokenDir = os.path.join(os.getcwd(), tokenName)

            if os.path.exists(tokenDir) == False:
                os.mkdir(tokenDir)

            text_file = open(tokenDir + "/description.xml", "w")
            n = text_file.write(descriptor_xml)
            text_file.close()

            text_file = open(tokenDir + "/extra.xml", "w")
            n = text_file.write(extra)
            text_file.close()

            shutil.make_archive(f'{tokenName}_archive', 'zip', tokenDir)

            token_zip_archive = f'{tokenName}_archive.zip'
            archive_filename = f'{self.archiveReports}/{jobID}/{runID}/{jobID}_{runID}_token.zip'
            self.save_in_archive(archive_filename=archive_filename,
                                 file=token_zip_archive,
                                 jobID=jobID,
                                 runID=runID)

            bc_req_response, success_cb_req = self.blockchain_handler.blockchain_request(action='notarize',
                                                                                         file_path=token_zip_archive,
                                                                                         file_name=f'{jobID}_{runID}_token.zip')
            os.remove(token_zip_archive)


            if datamartFileName.endswith('.zip'):
                extra_info = xmltodict.parse(extra)
                if extra_info['data']['@dataLocation'] == 'URL':
                    zip_url = extra_info['data']['#text']
                    r = requests.get(zip_url, stream=True)
                    with open(f'{tokenDir}/{datamartFileName}', 'wb') as fd:
                        for chunk in r.iter_content(chunk_size=128):
                            fd.write(chunk)
            elif datamartFileName.endswith('.csv'):
                data_retrieval(self.dataStoragePath, self.archiveInfoPath, datamartFileName, query, jobID, url_MSG,
                               self.messageLogFolder).to_csv(
                    tokenDir + "/" + datamartFileNameStandard, index=False)

            shutil.make_archive(tokenName, 'zip', tokenDir)

            token_zip = tokenName + ".zip"

            logger.debug(f'token zip: {token_zip}')
            shutil.move(token_zip, processorInputDir)

            shutil.rmtree(tokenDir)

            logger.info(f'Token READY for job {jobID}, runID {runID}. Datamart:{datamart}, Algorithm:{algorithm}')

            return "Running"
        else:
            logger.warning(
                f'User {usrToken} cannot access selected resources for job {jobID}, runID {runID}. Datamart:{datamart}, Algorithm:{algorithm}')
            raise ValueError(
                f'User {usrToken} cannot access selected resources for job {jobID}, runID {runID}. Datamart:{datamart}, Algorithm:{algorithm}')

    def record_new_tasks(self, df, excel_path):
        df_excel = pd.read_csv(excel_path)
        result = pd.concat([df_excel, df], ignore_index=True)
        result.to_csv(excel_path, index=False)
        return result

    @staticmethod
    def remove_all_tasks(self, excel_path):
        df_excel = pd.read_csv(excel_path)
        df_excel.head(0).to_csv(excel_path, index=False)


    @staticmethod
    def output_unzip(outputfold, token_out, task):
        unzip_fold = outputfold + "output_task_" + task
        os.mkdir(unzip_fold)
        logger.info(f"Folder created: {unzip_fold}")

        with zipfile.ZipFile(outputfold + token_out, 'r') as zip_ref:
            zip_ref.extractall(unzip_fold)
        logger.info(f"Files extracted in: {unzip_fold}")
        output_files = listdir(unzip_fold)
        logger.debug(output_files)
        output_html_files = [file for file in output_files if ".html" in file]
        output_zipped_files = outputfold + token_out
        if len(output_html_files) == 1:
            return unzip_fold + "/" + output_html_files[0], unzip_fold, output_zipped_files
        else:
            return "Error - multiple html files in the output folder"

    def postOutput(self, id, runID, out_filename):
        task_info = pd.read_csv(self.archiveInfoPath + "TASK_list_log.csv").fillna('')
        url = task_info[task_info["ID"].astype(str) == str(id)]["MessageDetailTableURL"].iloc[0]
        proxyAuthKey = task_info[task_info["ID"].astype(str) == str(id)]["ProxyAuthKey"].iloc[0]
        extension = out_filename.split(".")[-1]
        if extension == 'gpg':
            extension = out_filename.split(".")[-2] + '.' + extension

        if proxyAuthKey == '':
            hubURL = task_info[task_info["ID"].astype(str) == str(id)]["MessageURL"].iloc[0]
            fileName_short = id + '.' + runID + '.' + extension
        else:
            job_details = pull_url(url)
            if job_details != (None, None):
                hubURL = job_details[0]["output"]["hubURL"] + '?proxyAuthKey=' + proxyAuthKey
            else:
                logger.error(f'Failed retrieving url for posting output for Job Id {id} and Run Id {runID}')
                raise RuntimeError(f'Failed retrieving url for posting output for Job Id {id} and Run Id {runID}')
            fileName_short = id + '.' + runID + '.' + extension

        fileName = "/".join(out_filename.split("/")[:-1]) + "/" + fileName_short
        os.rename(out_filename, fileName)

        return post_file(hubURL, fileName_short, fileName, extension)

    def logOutput(self, r, task, runID, enc=False, bc_id=None):
        task_info = pd.read_csv(self.archiveInfoPath + "TASK_list_log.csv")
        url_MSG = task_info[task_info["ID"].astype(str) == str(task)]["MessageURL"].iloc[0]
        if r.text.strip()[:3] == "ACK":
            completion_msg = '<task_response>"Completed"</task_response><bc_id>###NO_BC_ID###</bc_id>'
            if bc_id is not None:
                completion_msg = completion_msg.replace("###NO_BC_ID###", str(bc_id))
            logger.debug(completion_msg)
            send_status_message(task,
                                runID,
                                "info.status.job",
                                completion_msg,
                                url_MSG,
                                self.messageLogFolder,
                                enc)
            self.fetch_set_task_from_id(runID=runID, jobID=task, mod='set', cols='Status', value='Completed')
        else:
            send_status_message(task,
                                runID,
                                "info.status.job",
                                r.text.strip(),
                                url_MSG,
                                self.messageLogFolder,
                                enc)

    def fetch_set_task_from_id(self, runID, jobID, mod, cols=None, value=None):
        task_list = pd.read_csv(self.archiveInfoPath + "/TASK_list_log.csv", dtype={'ID': str, 'runID': str})
        output_task = None
        if mod == 'fetch':
            fetched_task = task_list.loc[(task_list["ID"] == str(jobID)) & (task_list["runID"] == str(runID))]
            if len(fetched_task.index) != 1:
                warnings.warn('More than one job with the same jobID and runID!')
            output_task = fetched_task
        elif mod == 'set':
            if cols and value:
                task_list.loc[(task_list["ID"] == str(jobID)) & (task_list["runID"] == str(runID)), cols] = value
                task_list.to_csv(self.archiveInfoPath + "/TASK_list_log.csv", index=False)
            else:
                warnings.warn('Not acceptable values for Task_list_log setting input parameters!')
            output_task = task_list
        return output_task

    def send_description_message(self, job, url_MSG):
        job_descr, job_descr_xml = pull_url(job["MessageDetailTableURL"])

        if job_descr is not None:
            message_dict = {'get.datamart.desc': 'itemID',
                            'get.algorithm.desc': 'itemID',
                            'get.processor.num': 'itemID',
                            'delete_scheduled_job': 'itemID'}

            id = job_descr["@ID"]
            try:
                msgType = job_descr["@msgType"]
            except:
                msgType = job_descr["msg_type"]
            try:
                item = job_descr[message_dict[msgType]]
            except:
                logger.warning("Message type is not supported!")
                return
            if msgType == "get.datamart.desc":
                path = self.archiveInfoPath + "datamartDescription\\"
                self.send_description(path, item, url_MSG)
            elif msgType == "get.algorithm.desc":
                path = self.archiveInfoPath + "algorithmDescription\\"
                self.send_description(path, item, url_MSG)
            elif msgType == "get.processor.num":
                processorList = self.archiveInfoPath + "processor_list.csv"
                self.send_processors_info(id, msgType, processorList, self.messageLogFolder, url_MSG)
            elif msgType == 'delete_scheduled_job':
                self.delete_scheduled_job(item, url_MSG)
            return "Completed"
        else:
            return "Failed retrieving job management description"

    def delete_scheduled_job(self, item, url_MSG):
        df = pd.read_csv(self.archiveInfoPath + 'TASK_list_scheduled.csv')
        df = df[df['ID'].astype(str) != item]
        df.to_csv(self.archiveInfoPath + 'TASK_list_scheduled.csv', index=False)

    def send_processors_info(self, msgID, infoType, processorList, messageLogFolder,
                             url):
        processorList = pd.read_csv(processorList)
        if infoType == "get.processor.num":
            resp_msg = """<xml>
                          <msg ID='###ID###' msgType='get.processors.num'>
                            <running>###nRunning###</running>
                            <stopped>###nStopped###</stopped>
                          </msg>
                        </xml>"""
            try:
                resp_msg = resp_msg.replace("###nRunning###",
                                            str(processorList["Status"].value_counts().to_dict()["Running"]))
            except:
                resp_msg = resp_msg.replace("###nRunning###", "0")
            try:
                resp_msg = resp_msg.replace("###nStopped###",
                                            str(processorList["Status"].value_counts().to_dict()["Stopped"]))
            except:
                resp_msg = resp_msg.replace("###nStopped###", "0")

            resp_msg = resp_msg.replace("###ID###", str(msgID))
            filename = messageLogFolder + msgID + ".xml"
            text_message = open(filename, "w")
            text_message.write(resp_msg)
            text_message.close()
            response = post_file(url, filename, filename, 'text/xml')

    def send_description(self, path, item, url_MSG):
        items = os.listdir(path)
        items = [el for el in items if item in el or item == "*"]
        for item in items:
            filename = path + item
            response = post_file(url_MSG, filename, filename, 'text/xml')

    def processor_selector(self, agentRequesting):
        proc_list = pd.read_csv(self.archiveInfoPath + "/processor_list.csv")
        if agentRequesting == "TokenPreparation":
            proc_avail = proc_list[proc_list["Status"] == "Running"].sort_values("nRunningProcesses").reset_index(
                drop=True)
            return proc_avail.loc[0]["Proc_ID"], proc_avail.loc[0]["Processor_name"], proc_avail.loc[0][
                "processorInputDir"], \
                proc_avail.loc[0]["processorSyncDir"], proc_avail.loc[0]["processorOutputDir"]
        elif agentRequesting == "OutputPosting":
            proc_running = proc_list[(proc_list["Status"] == "Running")][[
                'Proc_ID', "processorOutputDir"]]
            return proc_running
        elif agentRequesting == "LogPosting":
            proc_running = proc_list[(proc_list["Status"] == "Running")][
                ['Proc_ID', "Processor_name", "Location", "processorSyncDir"]]
            return proc_running

    def processor_update_nRunningProcesses(self, proc_id, update_type):
        proc_list = pd.read_csv(self.archiveInfoPath + "/processor_list.csv")
        if update_type == "add":
            proc_list.loc[proc_list["Proc_ID"] == proc_id, "nRunningProcesses"] = proc_list["nRunningProcesses"] + 1
        elif update_type == "delete":
            proc_list.loc[proc_list["Proc_ID"] == proc_id, "nRunningProcesses"] = proc_list["nRunningProcesses"] - 1

    def run_task_now(self, jobID, runID, usrToken, datamart, algorithm, extra, query, url_MSG):

        try:
            proc_id, proc_name, processorInputDir, processorSyncDir, processorOutputDir = self.processor_selector(
                "TokenPreparation")
            status = self.token_preparation(jobID, runID, usrToken, datamart, algorithm, extra, query,
                                            processorInputDir,
                                            url_MSG)
            if status == 'Running':
                mess_type = "info.assigned.proc"
                self.processor_update_nRunningProcesses(proc_id, "add")
                send_status_message(jobID, runID, mess_type, str(proc_id), url_MSG, self.messageLogFolder)
                time.sleep(1)

        except Exception as e:
            logger.error(e)
            status = "No available processors!"
            proc_id = ""

        return status, proc_id

    def new_task_management(self, gui):

        proxyAuthKey = gui['proxyID']

        if gui['Name'] == 'Scheduler':
            url_MT = gui['MasterTableURL']
            url_MD = gui['MessageDetailTable']
            url_MSG = gui['MessageURL']
        else:
            url_MT = gui['MasterTableURL'] + '?proxyAuthKey=' + gui['proxyID']
            url_MD = gui['MessageDetailTable'] + '?proxyAuthKey=' + gui['proxyID']
            url_MSG = gui['MessageURL'] + '?proxyAuthKey=' + gui['proxyID']

        new_tasks = self.new_task_list_read(url_MT, url_MD, url_MSG, proxyAuthKey,
                                            self.archiveInfoPath + "TASK_list_log.csv")

        if new_tasks is not None:
            self.manage_new_ver_tasks(new_tasks, url_MSG)

            assignedProc, blockchain_info, enc_info, tasks_status = self.manage_new_proc_tasks(new_tasks, url_MSG)

            new_tasks.loc[new_tasks["Queue"].str.upper() == "PROCESSOR", "Status"] = tasks_status
            new_tasks.loc[new_tasks["Queue"].str.upper() == "PROCESSOR", "Processor"] = assignedProc
            new_tasks.loc[new_tasks["Queue"].str.upper() == "PROCESSOR", "runBlockChain"] = blockchain_info
            new_tasks.loc[new_tasks["Queue"].str.upper() == "PROCESSOR", "fileCrypting"] = enc_info

            tasks_status = self.manage_new_monitor_tasks(new_tasks, tasks_status, url_MSG)

            new_tasks.loc[new_tasks["Queue"].str.upper() == "MONITOR", "Status"] = tasks_status

            self.record_new_tasks(new_tasks, self.archiveInfoPath + 'TASK_list_log.csv')
        else:
            logger.error('Cannot retrieve new tasks!')
            print('Cannot retrieve new tasks!')


    def manage_new_monitor_tasks(self, new_tasks, tasks_status, url_MSG):
        tasks_status = []
        for index, job in new_tasks[new_tasks["Queue"].str.upper() == "MONITOR"].iterrows():
            task = str(job["ID"])
            runID = str(job['runID'])
            try:
                status = self.send_description_message(job, url_MSG)
            except Exception as e:
                status = e
            time.sleep(1)
            send_status_message(task, runID, "info.status.job", str(status), url_MSG, self.messageLogFolder)
            tasks_status.append(status)
        return tasks_status

    def manage_new_proc_tasks(self, new_tasks, url_MSG):
        tasks_status = []
        assignedProc = []
        blockchain_info = []
        enc_info = []
        for index, job in new_tasks[new_tasks["Queue"].str.upper() == "PROCESSOR"].iterrows():
            task = str(job["ID"])
            runID = str(job['runID'])

            task_info_dict = pull_task_info(job, self.archiveInfoPath)

            if task_info_dict['id'] != 'Error':
                if task_info_dict['scheduling']['@runNow'] == 'yes':
                    logger.info(f"Run Now for task {task} and run {runID}")
                    status, proc_id = self.run_task_now(task_info_dict['id'],
                                                        runID,
                                                        task_info_dict['usrToken'],
                                                        task_info_dict['datamart'],
                                                        task_info_dict['algorithm'],
                                                        task_info_dict['extra'],
                                                        task_info_dict['query'],
                                                        url_MSG)

                if [task_info_dict['scheduling']['months'],
                    task_info_dict['scheduling']['days'],
                    task_info_dict['scheduling']['hours'],
                    task_info_dict['scheduling']['mins']] != [None, None, None, None]:
                    logger.info(f"Scheduling task {task} and run {runID}")
                    status = self.prepare_for_scheduling(job, task_info_dict['job_descr_xml'],
                                                         task_info_dict['scheduling'])
                    proc_id = ''
            else:
                status = 'Error'
                proc_id = 'Error'

            tasks_status.append(status)
            assignedProc.append(proc_id)
            blockchain_info.append(task_info_dict['runBlockChain'])
            enc_info.append(task_info_dict['fileCrypting'])
            mess_type = "info.status.job"
            send_status_message(task, runID, mess_type, str(status), url_MSG, self.messageLogFolder)
        return assignedProc, blockchain_info, enc_info, tasks_status

    def manage_new_ver_tasks(self, new_tasks, url_MSG):
        for index, job in new_tasks[new_tasks["Queue"] == "Signature"].iterrows():
            job_ver_id = str(job['ID'])
            run_ver_ID = str(job['runID'])
            ver_task_info_dict = pull_ver_info(job)
            if ver_task_info_dict['id'] != 'Error':
                get_sign_res = requests.get(ver_task_info_dict["resourceURL"])
                if get_sign_res.status_code == 200:
                    file_signed = get_sign_res.content
                    with open(
                            f'{self.archiveReports}/{ver_task_info_dict["runRelatedJobID"]}/{ver_task_info_dict["proxyRunID"]}/{ver_task_info_dict["runRelatedJobID"]}.{ver_task_info_dict["proxyRunID"]}.html.sig',
                            'wb') as f:
                        f.write(file_signed)

                    sign_res, return_code = self.blockchain_handler.verify_signature(
                        path_to_verify=f'{self.archiveReports}/{ver_task_info_dict["runRelatedJobID"]}/{ver_task_info_dict["proxyRunID"]}/{ver_task_info_dict["runRelatedJobID"]}.{ver_task_info_dict["proxyRunID"]}.html.sig')

                    if return_code == 0:
                        logger.info(
                            f'Verified jobID:{ver_task_info_dict["runRelatedJobID"]}-runID:{ver_task_info_dict["proxyRunID"]}')
                        new_tasks.loc[
                            (new_tasks["ID"] == job_ver_id) & (new_tasks["runID"] == run_ver_ID), "Status"] = "Verified"
                        self.fetch_set_task_from_id(runID=ver_task_info_dict["proxyRunID"],
                                                    jobID=ver_task_info_dict["runRelatedJobID"], mod='set',
                                                    cols='Status',
                                                    value='Verified')
                        content = f'<verResponse>"Verified"</verResponse> <runRelatedJobID>{ver_task_info_dict["runRelatedJobID"]}</runRelatedJobID> <proxyRunID>{ver_task_info_dict["proxyRunID"]}</proxyRunID>'
                        send_status_message(task=job_ver_id,
                                            runID=run_ver_ID,
                                            mess_type='sig_req_ack',
                                            content=content,
                                            url=url_MSG,
                                            messageLogFolder=self.messageLogFolder, )
                    else:
                        logger.info(
                            f'Failed signature verification jobID:{ver_task_info_dict["runRelatedJobID"]}-runID:{ver_task_info_dict["proxyRunID"]}')
                        new_tasks.loc[
                            (new_tasks["ID"] == job_ver_id) & (new_tasks["runID"] == run_ver_ID), "Status"] = "Failed"
                        content = f'<verResponse>"Failed"</verResponse> <runRelatedJobID>{ver_task_info_dict["runRelatedJobID"]}</runRelatedJobID> <proxyRunID>{ver_task_info_dict["proxyRunID"]}</proxyRunID>'
                        send_status_message(task=job_ver_id,
                                            runID=run_ver_ID,
                                            mess_type='sig_req_ack',
                                            content=content,
                                            url=url_MSG,
                                            messageLogFolder=self.messageLogFolder)
                else:
                    content = f'<verResponse>{get_sign_res.text.strip()}</verResponse> <runRelatedJobID>{ver_task_info_dict["runRelatedJobID"]}</runRelatedJobID> <proxyRunID>{ver_task_info_dict["proxyRunID"]}</proxyRunID>'
                    send_status_message(task=job_ver_id,
                                        runID=run_ver_ID,
                                        mess_type="info.status.job",
                                        content=content,
                                        url=url_MSG,
                                        messageLogFolder=self.messageLogFolder,
                                        )
            else:
                new_tasks.loc[
                    (new_tasks["ID"] == job_ver_id) & (new_tasks["runID"] == run_ver_ID), "Status"] = "NoInfo"
                logger.error(f'Failed pulling verification task info for Job Verification Id: {job_ver_id} and Run '
                             f'Verification Id: {run_ver_ID}')

    def new_output_management(self):
        proc_list = self.processor_selector("OutputPosting")
        for index, proc in proc_list.iterrows():
            outputs = [f for f in os.listdir(proc['processorOutputDir']) if f.endswith('.zip')]
            if len(outputs) <= 0:
                pass
            else:
                tasks = [el.split(".")[0].replace("tUID_", "") for el in outputs]
                for token_out, task in zip(outputs, tasks):
                    bc_id = None
                    jobID = task.split('_')[0]
                    runID = task.split('_')[1]
                    html_filename, unzipfold, output_zipped_files = Proxy.output_unzip(proc['processorOutputDir'],
                                                                                       token_out, task)
                    archive_filename_html = f'{self.archiveReports}/{jobID}/{runID}/{jobID}.{runID}.html'
                    archive_filename_zip = f'{self.archiveReports}/{jobID}/{runID}/{jobID}.{runID}.zip'
                    self.save_in_archive(archive_filename=archive_filename_html,
                                         file=html_filename,
                                         jobID=jobID,
                                         runID=runID)
                    self.save_in_archive(archive_filename=archive_filename_zip,
                                         file=output_zipped_files,
                                         jobID=jobID,
                                         runID=runID)
                    encrypt = self.fetch_set_task_from_id(runID=runID, jobID=jobID, mod='fetch')['fileCrypting'].iloc[0] == 'yes'
                    if encrypt:
                        signed_html_file = f'{html_filename}.gpg'
                        sign_resp, return_code = self.blockchain_handler.sign_report(html_filename)
                        if return_code == 0 and os.path.isfile(signed_html_file):
                            html_filename = signed_html_file
                            shutil.copyfile(signed_html_file,
                                            f'{self.archiveReports}/{jobID}/{runID}/{jobID}.{runID}.html.gpg')
                        else:
                            logger.error(f'Failed signature with output {sign_resp.args}')

                    if self.fetch_set_task_from_id(runID=runID, jobID=jobID, mod='fetch')['runBlockChain'].iloc[
                        0] == 'yes':
                        bc_req_response, success_cb_req = self.blockchain_handler.blockchain_request(action='notarize',
                                                                                                     file_path=html_filename,
                                                                                                     file_name=f'output_report_{task}.html')
                        if bc_req_response['identifier'] is not None:
                            bc_id = bc_req_response['identifier']

                    try:
                        r_zip = self.postOutput(jobID, runID, archive_filename_zip)
                        logger.info(f'Results successfully sent for jobID {jobID} and runID {runID}')
                        print(f'Results successfully sent for jobID {jobID} and runID {runID}')
                        self.logOutput(r=r_zip, task=jobID, runID=runID, enc=encrypt, bc_id=bc_id)
                        os.remove(proc['processorOutputDir'] + "/" + token_out)
                        self.processor_update_nRunningProcesses(proc["Proc_ID"], "delete")
                    except Exception as e:
                        self.logOutput(r=f'Error managing output with jobID: {jobID} and runID: {runID} with error: {e}',
                                       task=jobID, runID=runID, enc=encrypt, bc_id=bc_id)
                        logger.error(f'Error posting the output with jobID: {jobID} and runID: {runID} with error: {e}')
                        print(f'Error posting the output with jobID: {jobID} and runID: {runID} with error: {e}')

                    logger.info(f"Cleaning: {unzipfold}")
                    print(f"Cleaning: {unzipfold}")
                    shutil.rmtree(unzipfold)

    def save_in_archive(self, archive_filename, file, jobID, runID):
        if not os.path.isdir(f'{self.archiveReports}/{jobID}/{runID}'):
            os.makedirs(f'{self.archiveReports}/{jobID}/{runID}')
        shutil.copyfile(file, archive_filename)

    def log_management(self, log_memory, archiveInfoPath):

        proc_list = self.processor_selector("LogPosting")
        for index, proc in proc_list.iterrows():
            for item in os.scandir(proc['processorSyncDir'] + "/processes/log"):
                task_id = item.name.split(".")[0]
                task_info = pd.read_csv(archiveInfoPath + "TASK_list_log.csv")
                try:
                    url = task_info[task_info["ID"] == task_id]["MessageURL"].iloc[0]
                    if log_memory[item.path] != item.stat().st_atime:
                        response = post_file(url, str(proc["Proc_ID"]) + "_" + proc["Location"] + "_" + item.name,
                                             item.path, 'text/xml')

                except:
                    try:
                        url = task_info[task_info["ID"] == task_id]["MessageURL"].iloc[0]
                        log_memory[item.path] = item.stat().st_atime
                        response = post_file(url, str(proc["Proc_ID"]) + "_" + proc["Location"] + "_" + item.name,
                                             item.path, 'text/xml')

                    except:
                        pass

    def stop_proxy(self):
        logger.info('Stopping Proxy!')
        self.authenticator.cancel_timer()
        logger.info('Authenticator timer canceled')
        del self.authenticator
        logger.info('Authenticator deleted')
        exit()
