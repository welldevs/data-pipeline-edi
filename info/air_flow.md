# Environment Setup

We will install and configure Airflow using Docker + Docker Compose, which is the most practical and recommended way.

## Step 1: Install Dependencies

Before we begin, we need to ensure that you have the following installed on your system:

✅ Docker → To run Airflow services.  
✅ Docker Compose → To manage the containers.  
✅ Git (optional) → For version control and code organization.

If you don't have them installed, use the following commands:

### For Debian/Ubuntu systems:
```sh
sudo apt update && sudo apt install docker.io docker-compose git -y
```

If you are on Windows, it is recommended to install WSL2 and then Docker Desktop.

## Step 2: Create Airflow Structure

Now let's configure the Airflow directory:

```sh
mkdir -p ~/airflow && cd ~/airflow
```

Download the official `docker-compose.yaml` for Airflow:

```sh
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
```

Now create the directories for configuration:

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

This sets up an environment ready to run Airflow in Docker.

## Step 3: Configure Environment Variables

Open the `.env` file and ensure it contains something like this:

```sh
AIRFLOW_UID=50000
```

If you are on Windows, replace `50000` with your user ID in WSL2.

## Step 4: Start Airflow Containers

Now let's start Airflow with Docker containers:

```sh
docker-compose up -d
```

This will start the following services:

- `airflow-webserver` → Web interface to manage DAGs.
- `airflow-scheduler` → Responsible for scheduling and running DAGs.
- `airflow-worker` → Executes pipeline tasks.
- `airflow-postgres` → Internal database for Airflow.
- `airflow-redis` → Manages task queues.

Wait a few minutes for Airflow to be ready.

## Step 5: Access the Airflow Interface

Open your browser and go to:
👉 [http://localhost:8080](http://localhost:8080)

Default user: `airflow`  
Default password: `airflow`

If everything is set up correctly, you will see the Airflow web interface. 🎉

## Step 6: Create an Initial DAG

Now, let's create a DAG (Directed Acyclic Graph) that will run our pipeline.

Create a file inside the `dags/` directory:

```sh
nano ~/airflow/dags/pipeline_edi.py
```

Paste the basic DAG code to test:

```python
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 3, 12),
    'catchup': False
}

with DAG('pipeline_edi',
         default_args=default_args,
         schedule_interval='@daily',  
         catchup=False) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> end
```

Now restart Airflow:

```sh
docker-compose restart
```

Within the Airflow interface ([http://localhost:8080](http://localhost:8080)), activate the `pipeline_edi` DAG and watch it run.