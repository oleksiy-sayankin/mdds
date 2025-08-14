# Modeling the dynamics of distributed systems

Dissertation in Glushkov Institute of Cybernetic of NAS of Ukraine. Contains code for solving equation systems.

## Installation guide

### Ubuntu 24.x.x

Requirements

```
sudo apt update
sudo apt install python3-pip python3-full git libblas-dev liblapack-dev nodejs npm
sudo npm install -g prettier
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
source mdds_env/bin/activate
```

4. Run server in console

```
make run_all
```

Open link in browser http://127.0.0.1:8000/

5. Create sample data files

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

6. Upload files and press "Solve" button

7. Use _Download Solution_ button to see the result