<!-- 
Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
Refer to the LICENSE file in the root directory for full license details.
-->

# 🚀 Modeling the dynamics of distributed systems

Dissertation in Glushkov Institute of Cybernetic of NAS of Ukraine. Contains code for solving equation systems.

## 👤 Users Installation Guide

### 🐧 Ubuntu

1. Install Docker 

[Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

2. Start Docker Daemon
```
sudo systemctl start docker
```

3. Pull Docker Image

```
docker pull oleksiysayankin/mdds:latest
```
4. Run MDDS Server
```
docker run -p 8000:8000 oleksiysayankin/mdds:latest
```
5. Open link in browser http://127.0.0.1:8000/

6. Create sample data files

Create two text files: _matrix.csv_:

```
3,2
1,4
```

and _vector.csv_:

``` 
10
8
```

7. Upload files and press _Solve_ button

8. Use _Download Solution_ button to see the result

## 💻 Developers Installation Guide

### 🐧 Ubuntu

Requirements

```
sudo apt update
sudo apt install python3-pip python3 python3-venv git libblas-dev liblapack-dev docker.io docker-compose-plugin shfmt shellcheck rabbitmq-server redis-server libxml2-utils
```
Java
```
wget "https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.deb"
sudo apt install ./jdk-21_linux-x64_bin.deb

mkdir ~/google/google-java-format
wget https://github.com/google/google-java-format/releases/download/v1.23.0/google-java-format-1.23.0-all-deps.jar -O ~/google/google-java-format/google-java-format.jar
```

1. Clone repository

```
git clone git@github.com:oleksiy-sayankin/mdds.git
cd mdds
```

2. Install Nodejs 22 LTS, nvm, npm

See [Download Node.js®](https://nodejs.org/en/download).


2. Install npm dependencies

```
npm install
```

3. Create Pyton env an install libraries

```
make setup_python_env
```

4. Activate python environment
```
source ~/.venvs/mdds/bin/activate
```

5. Enable and start RabbitMQ server
``` 
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
```

6. Enable and start Redis server
``` 
sudo systemctl enable redis-server
sudo systemctl start redis-server
```

7. Install SonarQube Server

See [here](https://docs.sonarsource.com/sonarqube-server/latest/server-installation/introduction/).
Create SonarQube token and put it to  `.sonar_token` file in root of the project.

8. Run server in console

```
make run_all
```

## 📜 License

This project is licensed under a **dual-license model**:

- ✅ **Free for personal, academic, and experimental use** under the  
  [PolyForm Noncommercial License 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0/).

- ❌ **Not permitted** for commercial or production use under the non-commercial license.

- 💼 **Commercial use available** via a separate license agreement.  
  Please contact **Oleksiy Oleksandrovych Sayankin** for details.

👉 See the full text in the [LICENSE](./LICENSE) file.