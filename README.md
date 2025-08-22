# Modeling the dynamics of distributed systems

Dissertation in Glushkov Institute of Cybernetic of NAS of Ukraine. Contains code for solving equation systems.

## Users Installation Guide

### Ubuntu 24.x.x

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

## Developers Installation Guide

### Ubuntu 24.x.x

Requirements

```
sudo apt update
sudo apt install python3-pip python3-full git libblas-dev liblapack-dev nodejs npm
sudo apt install docker-compose
sudo npm install -g prettier
sudo npm install eslint @eslint/js globals @eslint/json @eslint/markdown @eslint/css --save-dev
sudo npm install --save-dev jest jsdom jest-environment-jsdom
sudo npm install --save-dev babel-jest @babel/core @babel/preset-env
sudo npm install --save-dev eslint-plugin-jest
sudo npm install --save-dev bats
```

1. Clone repository

```
git clone git@github.com:oleksiy-sayankin/mdds.git
```

2. Create Pyton env an install libraries

```
cd mdds
make setup_python_env
```

3. Activate python environment
```
source source ~/.venvs/mdds/bin/activate
```

4. Run server in console

```
make run_all
```