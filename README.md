# Modeling the dynamics of distributed systems

Dissertation in Glushkov Institute of Cybernetic of NAS of Ukraine. Contains code for solving equation systems.

Ubuntu 24.x.x
-------------

Requirements
```
sudo apt install python3-pip python3-full git
```
1. Clone repository 
```
git clone https://github.com/oleksiy-sayankin/dissertation-code.git
```
2. Create Pyton env an install libraries
```
cd dissertation-code/slae_solver
python3 -m venv disser_env
source disser_env/bin/activate
pip install -r requirements.txt
```
3. Run server in console
```
uvicorn server:app --reload
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
7. Use _Download Solution_ link to see the result