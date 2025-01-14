import threading
import json
from Proxy.src.sbcommlib import *


class Authenticator:
    def __init__(self, send_status_message_func, messageLogFolder, GUI_list, authhost, auth_logger):
        self._lock = threading.Lock()
        self._timer = None
        self.is_running = False
        self.exec_time = 5
        self.auth_req_list = []
        self.detail_list = []
        self.GUI_list = GUI_list[GUI_list['Name'] != 'Scheduler']
        self.authhost = authhost
        self.send_status_message = send_status_message_func
        self.messageLogFolder = messageLogFolder
        self.logger = auth_logger

    def get_auth_req(self):
        if len(self.GUI_list) > 0:
            for index, gui in self.GUI_list.iterrows():
                if self.is_running and gui['Name'] != 'Scheduler':
                    with self._lock:
                        url_mmt = gui['MasterTableURL'] + '?proxyAuthKey=' + gui['proxyID']
                        url_mmd = gui['MessageDetailTable'] + '?proxyAuthKey=' + gui['proxyID']
                        url_msg = gui['MessageURL'] + '?proxyAuthKey=' + gui['proxyID']
                        proxyAuthKey = gui['proxyID']

                        task_list = pull_new_tasks(url_mmt, url_mmd, url_msg, proxyAuthKey, pull_type=['AUTH'])
                        if len(task_list.index) > 0:
                            for row, job in task_list.iterrows():
                                job_dict = job.to_dict()
                                if (job_dict not in self.auth_req_list):
                                    self.auth_req_list.append(job_dict)
                                    self.send_status_message(str(job_dict['ID']), str(job_dict['runID']),
                                                             "auth_req_ack", 'Authenticating...',
                                                             url_msg, self.messageLogFolder)
                        if len(self.auth_req_list) > 0:
                            new_usr_token = self.process_auth_requests(gui)
                            if len(new_usr_token) > 0:
                                self.logger.info(f'Autenticated tokens: {new_usr_token}')
                    self._timer = threading.Timer(self.exec_time, self.get_auth_req)
                    self._timer.daemon = True  # Set the thread as daemon
                    self._timer.start()
                    if len(self.auth_req_list) > 0:
                        self.logger.info(f'This is the Authentication request list: {self.auth_req_list}')

    def process_auth_requests(self, gui):
        new_authenticated_users = []
        url_MMD = gui['MessageDetailTable'] + '?proxyAuthKey=' + gui['proxyID']
        url_MSG = gui['MessageURL'] + '?proxyAuthKey=' + gui['proxyID']
        for job in self.auth_req_list:
            ID = job['ID']

            job_descr, job_descr_xml = pull_url(url_MMD + '&jobID=' + str(ID))

            if job_descr is not None:
                assert job_descr['@ID'] == str(ID)
                job_detail = {'ID': job_descr['@ID'],
                              'msg_type': job_descr['msg_type'],
                              'usr_name': job_descr['usr_name'],
                              'psw': job_descr['psw'],
                              }
                if job_detail not in self.detail_list:
                    self.detail_list.append(job_detail)
            else:
                self.logger.error(f'Error retrieving Job Authentication Info for Gui {gui}, and Job {ID}')

        if len(self.detail_list) > 0:
            new_authenticated_users = self.process_auth_job(dashID=gui['dashID'], url_MSG=url_MSG)
        return new_authenticated_users

    def start_timer(self):
        self.is_running = True
        self._timer = threading.Timer(self.exec_time, self.get_auth_req)
        self._timer.daemon = True  # Set the thread as daemon
        self._timer.start()

    def cancel_timer(self):
        self.is_running = False
        if self._timer:
            self._timer.cancel()
            self.logger.info("Timer canceled.")

    def check_permissions(self, usrToken, datamartCID, algoDockerCID):
        # check datamart
        res, r, msg = Authenticator.auth_service_item_permission(self.authhost, usrToken, datamartCID)
        # check datamart
        res, t, msg = Authenticator.auth_service_item_permission(self.authhost, usrToken, algoDockerCID)
        if r & t:
            return True
        else:
            return False

    def auth_service_token_validation(self, usrToken):
        r = requests.get(
            'http://' + self.authhost + '/dashAuthPage/src/auth.service.token.validation.php?token' + usrToken)
        r = eval(r.text)
        return r['msg']['tokenAvailable']

    def process_auth_job(self, dashID, url_MSG):
        new_authenticated_users = []
        for detail_job in self.detail_list:
            usr = detail_job['usr_name']
            res, token = self.auth_service_autentication_usr_pwd_check(self.authhost, usr, detail_job['psw'],
                                                                       token=True)
            if res and token and res == 'yes':

                visibleItems = auth_query_items_by_token(self.authhost, token, itemType=None)
                if str(dashID) in visibleItems['cItem']:  # check if GUI is in visibleItems
                    content = "<token>" + token + "</token> <permission>" + visibleItems['xmltag'].str.cat(
                        sep=' ') + "</permission>"  # TODO salvare lista items
                    self.send_status_message(str(detail_job.get('ID')), '0', "auth_req_res", content, url_MSG,
                                             self.messageLogFolder)
                    new_authenticated_users.append(token)
                    status = f'Completed {usr}'
                else:
                    status = f'User {usr} is not authenticated for the GUI!'
                    self.send_status_message(str(detail_job.get('ID')), '0', "auth_req_res", '', url_MSG,
                                             self.messageLogFolder)
            else:
                status = f'User {usr} cannot be authenticated!'
                self.send_status_message(str(detail_job.get('ID')), '0', "auth_req_res", '', url_MSG,
                                         self.messageLogFolder)
            self.logger.info(status)
            self.clean_req_lists(detail_job.get('ID'))
        return new_authenticated_users

    def clean_req_lists(self, job_auth_id):
        list_of_jobs = []
        for _, job in enumerate(self.auth_req_list):
            if job['ID'] == job_auth_id:
                list_of_jobs.append(job)
        if len(list_of_jobs) > 0:
            if len(list_of_jobs) == 1:
                self.auth_req_list = [job for job in self.auth_req_list if job != list_of_jobs[0]]
            else:
                raise ValueError('Duplicated jobs in the list!')
        list_of_jobs = []
        for _, job in enumerate(self.detail_list):
            if job['ID'] == job_auth_id:
                list_of_jobs.append(job)
        if len(list_of_jobs) > 0:
            if len(list_of_jobs) == 1:
                self.detail_list = [detail for detail in self.detail_list if detail != list_of_jobs[0]]
            else:
                raise ValueError('Duplicated jobs in the list!')

    def auth_service_autentication_usr_pwd_check(self, authhost, usr, pwd, token=False):
        if token:
            token = 'true'
        else:
            token = 'false'
        r = requests.get(
            'http://' + authhost + '/dashAuthPage/src/auth.service.autentication.usr.pwd.check.php?user=' + usr + '&pwd=' + pwd + '&giveBackToken=' + token)
        content_str = r.content.decode("utf-8")
        start_json = content_str.find('{')
        json_str = content_str[start_json:]
        try:
            r_json = json.loads(json_str)
            return r_json['msg']['autentication'], r_json['token']
        except json.JSONDecodeError as e:
            self.logger.error(f'Error checking user for authentication for user {usr}')
            return None, None

    ### STATIC METHODS for Authentications ###
    @staticmethod
    def auth_service_item_permission(authhost, token, item):
        r = requests.get(
            'http://' + authhost + '/dashAuthPage/src/auth.service.item.permission.php?token=' + token + '&item=' + str(
                item))
        r = eval(r.text.replace('false', 'False').replace('true', 'True'))
        return r['msg'], r['result'], r['errorMsg']

def auth_query_items_by_token(authhost, token, itemType=None):
    itemTypenum = {'datamart': '5', 'algorithm': '6'}
    try:
        typeNum = itemTypenum[itemType]
    except:
        itemType = None

    if itemType is None:
        r = requests.get(
            'http://' + authhost + '/dashAuthPage/src/auth.query.items.by.token.php?token=' + token)
    else:
        r = requests.get(
            'http://' + authhost + '/dashAuthPage/src/auth.query.items.by.token.php?token=' + token + '&itemType=' + typeNum)
    df = pd.DataFrame(eval(r.text.replace('null', 'None'))).T
    df[['Tag', 'Source']] = df['itemCode'].replace('\n', ' ', regex=True).replace('\r', '', regex=True).str.extract(
        'TAG=(.*?)\sSOURCE=(.*$)')  # TAG=(.*?)\\n
    df.loc[df['tipo'] == '5', 'tipo'] = 'datamart'
    df.loc[df['tipo'] == '6', 'tipo'] = 'algorithm'
    df['xmltag'] = "<item objType='" + df['tipo'] + "' objName='" + df['Tag'] + "'></item>"

    return df[['cItem', 'Tag', 'tipo', 'descrizione', 'Source', 'xmltag']]

