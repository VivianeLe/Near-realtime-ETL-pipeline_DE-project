# Airflow setup
Since i am currently using windows, which does not support the installation of Airflow so i used Oracle VM VirtualBox to create a virtual Unbuntu Linux machine.
## Install Airflow
```
Sudo su
Sudo apt-get update
Sudo apt-get install python3-pip
pip install apache-airflow
cd /airflow
export AIRFLOW_HOME=$(pwd)
airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
airflow db init
airflow webserver -p 8080
airflow scheduler
mkdir dags
```
After that all dags showed up at ``` localhost:8080```

