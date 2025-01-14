# GSProxy
This repository contains the code of the Proxy Module, one of the main modules of the GEN-RWD Sandbox infrastructure (https://doi.org/10.1186/s12911-024-02549-5).

# Tutorial
It follows a step by step guide to deploy a Proxy Module with one Processor (https://doi.org/10.1109/ICHI61247.2024.00053) and B&C Agent (https://doi.org/10.3233/shti240643).

Processor repo: https://gitlab.com/benedetta.gottardelli/GSProcessor

B&C Agent repo: https://github.com/leocatnucc/distrib_block_crypto

## Requirements

- Create Private-Public GPG Key pair for signing and validation

``` bash 
# Install gpg
sudo apt-get install gnupg 

# Create gpg Key Pair
gpg --gen-key

# Check your key
gpg --list-secret-keys --keyid-format=long

# Export your public key. Save it no filesystem and share it with Sandbox' users. DON'T SHARE PRIVATE KEY
gpg --output output/path/to/your/public_key.gpg --export yourkeyemail@test.org

# Import users' public keys. Users must share their public key.
gpg --import usr_public_key.gpg

```

- Install R, RServer or Rstudio

``` bash 
#Install R
sudo apt-get install r-base  
```

``` bash 
#Download and install RServer
cd /
sudo mkdir downloads
cd downloads/
wget https://download2.rstudio.org/server/bionic/amd64/rstudio-server-2021.09.1-372-amd64.deb
sudo wget https://download2.rstudio.org/server/bionic/amd64/rstudio-server-2021.09.1-372-amd64.deb
sudo apt install ./rstudio-server-2021.09.1-372-amd64.deb
```
    
- Check python version --> Python 3.9 

``` bash 
python3 --version  
```
- Install docker

``` bash 
sudo apt-get remove docker docker-engine docker.io containerd runc
sudo apt-get update
sudo apt-get install     ca-certificates     curl     gnupg     lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo   "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo docker run hello-world 
```
```bash 
# add user to docker group and re-login 
sudo groupadd docker
sudo usermod -aG docker $USER
```

Reboot

## Installation

In the GSProxy folder:

1. Enter the repository folder 

``` bash 
cd GSProxy/
```

2. Download the Processor package

``` bash 
sudo mkdir Processor
cd Processor
sudo git clone https://gitlab.com/benedetta.gottardelli/GSProcessor.git
```

3. Install the processor R-package running in RStudio/RServer the following command in the repository folder (GSProxy/)

``` R 
install.packages("devtools")
devtools::install('./Processor/GSProcessor') # processor source code path
```
If devtools installation fails, try installing the following dependencies:

Debian : 
``` bash 
sudo apt-get update
sudo apt-get install libcurl4-openssl-dev libssl-dev libgit2-dev libfontconfig1-dev libfribidi-dev libxml2 libxml2-dev libharfbuzz-dev libfribidi-dev libtiff-dev
```
Ubuntu: 

``` bash 
sudo apt update
sudo apt install build-essential libcurl4-gnutls-dev libxml2-dev libssl-dev libgit2-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev

```

4. Create a python virtual enviroment

``` bash 
sudo apt install python3-pip
sudo apt install python3-venv
pip install virtualenv
python3 -m venv proxyvenv
source proxyvenv/bin/activate
pip install -r Proxy/requirements.txt
```
5. Download Docker images from DockerHub --> user: generatorsandbox password: benedetta

``` bash 
docker login #insert credentials
docker pull generatorsandbox/catalog:descrittivabase
docker tag generatorsandbox/catalog:descrittivabase descrittivabase
docker pull generatorsandbox/catalog:clustering
docker tag generatorsandbox/catalog:clustering clustering
```

# Use

1. Create Processor instance:

``` bash 
make processor NAME="<name>"
```
Example: 
``` bash 
make processor NAME="A"
```

2. Create Proxy instance:

``` bash 
make proxy NAME="<nome>"
```
Exemple: 
``` bash 
make proxy NAME="GEMELLI"
```

3. Populate Proxy archives:

``` bash 
make proxy_populate NAME="<nome>" DATA_FOLD="<data_folder>" ALG_FOLD="<algo_fold>"
```

In the folders there are some examples of datamarts and algorithms xml descriptors needed by the GUI. Detailed explanation can be found in the GEN-RWD Sandbox paper (https://doi.org/10.1186/s12911-024-02549-5). 
``` bash 
make proxy_populate NAME="GEMELLI" DATA_FOLD="Data" ALG_FOLD="Algo"
```

4. Associate Proxy and Processor instances: 

``` bash 
make associate  PROCID=<id_processor> PROC="<name_processor>" PROX="<name_proxy>"
```
Example: 
``` bash 
make associate  PROCID="001" PROC="A" PROX="GEMELLI"
```
5. Update the config.yaml file in the POC folder /sandboxpoc/config.yaml. Use as reference the config_ex.yaml file.

6. Update the GUI_configuration_file.csv. It must contain the info for every GUI the Proxy has to watch. Example:

| dashID | Name |   MasterTableURL   | MessageDetailTable | MessageURL | proxyID |
| :---: | :---: |:------------------:|:------------------:| :---: | :---: |
| 13 | HospitalName | http://GUI/MMT.php | http://GUI/MMD.php | http://GUI/msg.php | Abcd12345 |

7. Run Processor

``` bash 
make run_processor PROC="<name_processor>"
```
Example: 
``` bash 
make run_processor PROC="A"
```

8. Within the Proxy's virtual environment, run Proxy:

``` bash 
make run_proxy PROX="<name_proxy>"
```
Example: 
``` bash 
make run_proxy PROX="GEMELLI"
```
