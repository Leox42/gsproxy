import xmltodict
import pandas as pd
import requests
import os
import time
import xml.etree.ElementTree as ET

def is_valid_xml(xml_str):
    try:
        ET.fromstring(xml_str)
        return True
    except ET.ParseError:
        return False


def pull_url(
        url):
    try:
        r = requests.get(url)
    except requests.exceptions.Timeout as e:
        print(f'Error in GET REQUEST for url {url} with Timeout error {e}')
        return None, None
    except requests.exceptions.TooManyRedirects as e:
        print(f'Error in GET REQUEST for url {url} with TooManyRedirects error {e}')
        return None, None
    except requests.exceptions.ConnectionError as e:
        print(f'Error in GET REQUEST for url {url} with ConnectionError error {e}')
        return None, None
    except requests.exceptions.HTTPError as e:
        print(f'Error in GET REQUEST for url {url} with HTTPError error {e}')
        return None, None
    except requests.exceptions.RequestException as e:
        print(f'Error in GET REQUEST for url {url} with generic error {e}')
        return None, None

    r.raise_for_status()

    if r.text != '':
        r_content = r.text.replace("<></>", '<inputData></inputData>')
        if xmltodict.parse(r_content)['xml'] != None:
            return xmltodict.parse(r_content)["xml"]["job"], r.content
        else:
            return {}, r.content
    else:
        print(f'Error pulling url for {url}, response is empty {r.text}')
        return None, None


def pull_scp(url, archive_path):
    loc_pub = f'{archive_path}sandbox.pub'
    url_from_gui = url
    path_to_save = './auth_msg_detail_rec.xml'
    cmd = f'scp -i {loc_pub} {url_from_gui} {path_to_save}'
    os.system(cmd)
    with open('./auth_msg_detail_rec.xml', 'r') as f:
        data = f.read()
    os.remove('./auth_msg_detail_rec.xml')
    try:
        return xmltodict.parse(data)["xml"]["job"]
    except:
        return [xmltodict.parse(data)["xml"]["job"]]


def pull_new_tasks(url, url_MMD, url_MSG, proxyAuthKey, pull_type=['PROCESSOR', 'MONITOR', 'SIGNATURE']):
    tasks_from_url = pull_url(url)

    if tasks_from_url is not (None, None):
        new_tasks = pd.DataFrame(tasks_from_url[0],
                                 columns=['@ID', 'runID', 'name', 'queue', 'priority', 'source',
                                          'targetProxy']).reset_index(
            drop=True)
        if new_tasks['@ID'].nunique() != len(new_tasks):
            new_tasks = pd.DataFrame(tasks_from_url[0], columns=['@ID', 'runID', 'name', 'queue', 'priority', 'source',
                                                                 'targetProxy']).reset_index(drop=True).head(1)

        new_tasks['runID'] = new_tasks['runID'].fillna('0')

        new_tasks = new_tasks[new_tasks['queue'].astype(str).str.upper().isin(pull_type)]

        new_tasks["ID"] = new_tasks["@ID"]
        new_tasks["Name"] = new_tasks['name']
        new_tasks["Queue"] = new_tasks['queue']
        new_tasks["Priority"] = new_tasks['priority']
        new_tasks["MasterTableURL"] = url

        if proxyAuthKey != '':
            new_tasks["MessageDetailTableURL"] = url_MMD + '&jobID=' + new_tasks["ID"].astype(str)
        else:
            if len(new_tasks) > 0:
                new_tasks["MessageDetailTableURL"] = url_MMD + new_tasks["ID"].astype(str)
            else:
                new_tasks["MessageDetailTableURL"] = url_MMD

        new_tasks["MessageURL"] = url_MSG
        new_tasks["ProxyAuthKey"] = proxyAuthKey

        return new_tasks.drop(columns=['@ID', 'queue', 'name', 'priority', 'source']).dropna()
    else:
        print(f'Error pulling new tasks for url {url}, cannot retrieve new tasks from pull_url')
        return None


def pull_task_info(job, archive_path):
    job_descr, job_descr_xml = pull_url(job["MessageDetailTableURL"])

    if job_descr is not None:
        id = job_descr["@ID"]
        usrToken = job_descr["usrToken"]
        datamart = job_descr["input"]["datamart"]
        algorithm = job_descr["input"]["algorithm"]
        try:
            extra = xmltodict.unparse(
                {your_key: job_descr["input"][your_key] for your_key in job_descr["input"].keys() if
                 your_key in ['data']}, pretty=True)
        except:
            extra = xmltodict.unparse({'extra': 'niente'}, pretty=True)

        query = job_descr["input"]["query"]
        if query == None:
            query = ""
        scheduling = job_descr['scheduling']
        runBlockChain = job_descr['runBlockChain']
        fileCrypting = job_descr['fileCrypting']

        return {'id': id,
                'usrToken': usrToken,
                'datamart': datamart,
                'algorithm': algorithm,
                'extra': extra,
                'query': query,
                'scheduling': scheduling,
                'runBlockChain': runBlockChain,
                'fileCrypting': fileCrypting,
                'job_descr_xml': job_descr_xml}
    else:
        print(
            f'Error pulling task info for url {job["MessageDetailTableURL"]}, cannot retrieve tasks info from pull_url')
        error = 'Error'
        return {'id': error,
                'usrToken': error,
                'datamart': error,
                'algorithm': error,
                'extra': error,
                'query': error,
                'scheduling': error,
                'runBlockChain': error,
                'fileCrypting': error,
                'job_descr_xml': error}


def pull_ver_info(job):
    job_descr = pull_url(job["MessageDetailTableURL"])[0]
    if job_descr is not (None, None):
        return {
            'id': job_descr["@ID"],
            'usrToken': job_descr["usrToken"],
            'proxy': job_descr["proxy"],
            'userName': job_descr["userName"],
            'runID': job_descr["runID"],
            'resourceURL': job_descr['resourceURL'],
            'proxyRunID': job_descr['proxyRunID'],
            'runRelatedJobID': job_descr['runRelatedJobID'],
        }
    else:
        error = 'Error'
        return {
            'id': error,
            'usrToken': error,
            'proxy': error,
            'userName': error,
            'runID': error,
            'resourceURL': error,
            'proxyRunID': error,
            'runRelatedJobID': error,
        }

def post_file(url, fileName_short, fileName, type):
    files = [("file", (fileName_short, open(fileName, "rb"), type))]  ##
    response = requests.post(url, files=files)
    return response

def send_status_message(task, runID, mess_type, content, url,
                        messageLogFolder, enc=False):
    xml_str = """<xml>
    <job ID = '###task###'>
    <runID>'###runID###'</runID>
    <msg type='###messageType###'>
        <content  enc='###encryption###'>###content###</content></msg>
    </job>
    </xml>"""

    if mess_type == "info.status.job" and content not in ["Running", "Completed", "Scheduled", 'Sent to Scheduler']:
        mess_type = "info.error.job"

    xml_str = xml_str.replace("###task###", task)
    xml_str = xml_str.replace("###runID###", runID)
    xml_str = xml_str.replace("###messageType###", mess_type)
    xml_str = xml_str.replace("###content###", content)
    xml_str = xml_str.replace("###encryption###", str(enc))
    filename = messageLogFolder + "task_" + task + "_" + str(round(time.time())) + ".xml"
    text_message = open(filename, "w")
    text_message.write(xml_str)
    text_message.close()
    print(xml_str)

    response = post_file(url, filename, filename, 'text/xml')
    print(response.text)