import hashlib
import requests
import yaml
import subprocess

class BlockChainHandler:
    def __init__(self, bc_logger, config_file_path):

        with open(config_file_path, "r") as f:
            yaml_bc = yaml.safe_load(f)
        self.url = yaml_bc['BLOCKCHAIN']['url']
        self.suffix_login = yaml_bc['BLOCKCHAIN']['suffix_login']
        self.suffix_nota = yaml_bc['BLOCKCHAIN']['suffix_nota']
        self.suffix_logout = yaml_bc['BLOCKCHAIN']['suffix_logout']
        self.suffix_search = yaml_bc['BLOCKCHAIN']['suffix_search']
        self.hash_algorithm = yaml_bc['BLOCKCHAIN']['hash_algorithm']
        self.usrname = yaml_bc['BLOCKCHAIN']['username']
        self.password = yaml_bc['BLOCKCHAIN']['password']
        self.headers = yaml_bc['BLOCKCHAIN']['headers']
        self.sign_pubkey = yaml_bc['BLOCKCHAIN']['sign_pubkey']
        self.sign_key = yaml_bc['BLOCKCHAIN']['sign_key']
        self.sign_passphrase = yaml_bc['BLOCKCHAIN']['sign_passphrase']
        self.security_script_folder = yaml_bc['BLOCKCHAIN']['security_script_folder']
        self.logger = bc_logger


    def calculate_file_hash(self, filename, hash_algorithm="sha256", chunk_size=4096):
        """Calculate the hash of a file."""
        hash_obj = hashlib.new(hash_algorithm)

        with open(filename, 'rb') as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                hash_obj.update(chunk)

        file_hash = hash_obj.hexdigest()
        return file_hash

    def post_request(self, url, headers, data, action):
        token = None
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()  # Check if the request was successful (status code 2xx)
        except requests.exceptions.HTTPError as errh:
            self.logger.error(
                f'Failed post request in Blockchain Handler for url {url} with error HTTPError {errh}')
            return None, None
        except requests.exceptions.ConnectionError as errc:
            self.logger.error(
                f'Failed post request in Blockchain Handler for url {url} with error ErrorConnecting {errc}')
            return None, None
        except requests.exceptions.Timeout as errt:
            self.logger.error(
                f'Failed post request in Blockchain Handler for url {url} with error TimeoutError {errt}')
            return None, None
        except requests.exceptions.RequestException as err:
            self.logger.error(
                f'Failed post request in Blockchain Handler for url {url} with Error {err}')
            return None, None
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            self.logger.info(f"JSON Response for {action} in Blockchain: {response.json()}")
            if response.json().get('access_token') is not None:
                token = response.json()['access_token']
        return token, response

    def get_request(self, url, headers, action, data=None):
        token = None
        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()  # Check if the request was successful (status code 2xx)
        except requests.exceptions.HTTPError as errh:
            self.logger.error(
                f'Failed get request in Blockchain Handler for url {url} with error HTTPError {errh}')
            return None, None
        except requests.exceptions.ConnectionError as errc:
            self.logger.error(
                f'Failed get request in Blockchain Handler for url {url} with error Error Connecting {errc}')
            return None, None
        except requests.exceptions.Timeout as errt:
            self.logger.error(
                f'Failed get request in Blockchain Handler for url {url} with error Timeout Error {errt}')
            return None, None
        except requests.exceptions.RequestException as err:
            self.logger.error(
                f'Failed get request in Blockchain Handler for url {url} with Error {err}')
            return None, None
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            self.logger.info(f"JSON Response for {action} in Blockchain: ", response.json())
            if response.json().get('access_token') is not None:
                token = response.json()['access_token']
        return token, response

    def get_2_bc(self, action: str, token=None, file_path=None, file_name=None):
        response = None
        success = False

        if action == 'identifier':
            if token and file_name:
                url = self.url + self.suffix_nota + f'{file_name}'
                headers = self.headers
                headers['Authorization'] = f'Bearer {token}'
                _, response = self.get_request(url=url, headers=headers, action=action)
                if response.status_code == 200:
                    success = True
                else:
                    self.logger.error(f'Could not perform the search for action {action} and token {token}!')
            else:
                self.logger.error(f'Invalid token for action {action} and token {token}!')
        else:
            self.logger.error(f'Action {action} not supported!')
        return token, response, success

    def post_2_bc(self, action: str, token=None, file_path=None, file_name=None):
        response = None
        success = False

        if action == 'login':
            url = self.url + self.suffix_login
            data = {'username': self.usrname,
                    'password': self.password}
            token, response = self.post_request(url=url, headers=self.headers, data=data, action=action)
            if response:
                if response.status_code == 200:
                    success = True
            else:
                self.logger.error(f'Error for action {action}.')
        elif action == 'notarize':
            if token and file_path and file_name:
                url = self.url + self.suffix_nota
                headers = self.headers
                headers['Authorization'] = f'Bearer {token}'
                file_hash = self.calculate_file_hash(filename=file_path, hash_algorithm=self.hash_algorithm)
                self.logger.info(f'Notarizing file {file_name} with hash {file_hash}')
                data = {'name': file_name,
                        'hash': file_hash}
                _, response = self.post_request(url=url, headers=headers, data=data, action=action)
                if response:
                    if response.status_code == 200:
                        success = True
                    else:
                        self.logger.error(f'Token {token} or file path {file_path} not valid for notarization!')
                else:
                    self.logger.error(f'Error for action {action}.')
        elif action == 'logout':
            if token:
                url = self.url + self.suffix_logout
                data = {'global': False}
                headers = self.headers
                headers['Authorization'] = f'Bearer {token}'
                _, response = self.post_request(url=url, headers=headers, data=data, action=action)
                if response:
                    if response.status_code == 204:
                        success = True
                else:
                    self.logger.error(f'Error for action {action}.')
        elif action == 'search':
            if token and file_name:
                self.logger.info(f'Searching: {file_name}')
                url = self.url + self.suffix_search
                headers = self.headers
                headers['Authorization'] = f'Bearer {token}'
                data = {'name': file_name}
                _, response = self.post_request(url=url, headers=headers, data=data, action=action)
                if response:
                    if response.status_code == 200:
                        success = True
                    else:
                        self.logger.error(f'Could not perform the search! Token {token} or file name {file_name} not valid for notarization!')
                else:
                    self.logger.error(f'Error for action {action}.')
        else:
            self.logger.error(f'Action {action} not supported.')

        return token, response, success

    def blockchain_request(self, action, file_path=None, file_name=None):
        bc_req_response = None
        success_bc_req = None
        token, login_response, success_login = self.post_2_bc(action='login')
        if success_login and token:

            if action == 'identifier':
                _, bc_req_response, success_bc_req = self.get_2_bc(action=action, token=token, file_path=file_path,
                                                                        file_name=file_name)
            else:
                _, bc_req_response, success_bc_req = self.post_2_bc(action=action, token=token, file_path=file_path,
                                                                        file_name=file_name)
            if action == 'notarize':
                if success_bc_req and bc_req_response.json() and 'identifier' in bc_req_response.json():
                    self.logger.info(
                        f'Successfully notarized {file_name} file with identifier: {bc_req_response.json()["identifier"]}')
            elif action == 'search':
                if success_bc_req and bc_req_response:
                    self.logger.info(
                        f'Search results: {success_bc_req}, {bc_req_response.json()}')
            elif action == 'identifier':
                if success_bc_req and bc_req_response:
                    self.logger.info(
                        f'Search results: {success_bc_req}, {bc_req_response.json()}')
            _, logout_response, success_logout = self.post_2_bc(action='logout', token=token)
            return bc_req_response.json(), success_bc_req

    def sign_report(self, report_2_sign):

        try:
            process = subprocess.Popen([self.security_script_folder+'sign_document.sh'] + [self.sign_passphrase, report_2_sign],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       text=True)

            process.wait()
            output, error = process.communicate()
            return_code = process.returncode
            return process, return_code
        except Exception as e:
            self.logger.error("Error:", str(e))
            return str(e), None

    def verify_signature(self, path_to_verify):

        try:
            process = subprocess.Popen([self.security_script_folder+'verify_signature.sh'] + [path_to_verify],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       text=True)
            process.wait()
            output, error = process.communicate()
            return_code = process.returncode
            return process, return_code
        except Exception as e:
            self.logger.error("Error:", str(e))
            return str(e), None

