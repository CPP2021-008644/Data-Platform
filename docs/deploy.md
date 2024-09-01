
## How to deploy
Update the submodule with `git submodule update --init --recursive`
<details>
<summary>Expand for first run instructions.</summary>

### First run
To set up the databases you have the docker compose file `docker-compose_dtplt_firstrun.yaml`.

#### Secrets
For the first run you will need the following files.
In the `secrets/postgres` folder:
- `dtpltauth.sql` has the database credentials.
  For instance:
  ```sql
    ALTER USER dbadmn WITH PASSWORD 'FOO';
  ```
- `dtpltairflow.sql` creates the airflow database.  
    Is in secrets in order to avoid publishing the airflow database name and users.
  For instance:
  ```sql
    CREATE USER airflowad WITH PASSWORD 'BAR';

    CREATE DATABASE airflowdb OWNER airflowad;
  ```

- `dtpltpgfile` postgres password file used during the creation of the database.
  For instance:
  ```
    FOO
  ```
In the `secrets/minio` folder:
- `.envminio_firstrun` has the envvars with the credentials for the root user and the buckets we want to create.  
    For instance:
    ```.env
    #first run settings
    MINIO_ROOT_USER=foo
    MINIO_ROOT_PASSWORD=baaarrfoozzp
    MINIO_DEFAULT_BUCKETS=baz1;baz2
    ```
- `initial.sh` has the script to create buckets, policies, users, groups, and service accounts.
    For instance:
    ```bash
    ## Run the first time to set 
    # docker exec -it mini bash /var/run/secrets/minitial
    # bash /var/run/secrets/minitial

    mc alias set myminio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

    mc mb myminio/foo
    mc mb myminio/bar

    cat > /tmp/foo-readonly.json <<EOF
    {
    "Version": "2012-10-17",
    "Statement": [
        {
        "Action": [
            "s3:GetBucketLocation",
            "s3:ListBucket"
        ],
        "Effect": "Allow",
        "Resource": [
            "arn:aws:s3:::foo"
        ]
        },
        {
        "Action": [
            "s3:GetObject"
        ],
        "Effect": "Allow",
        "Resource": [
            "arn:aws:s3:::foo/*"
        ]
        }
    ]
    }
    EOF

    cat > /tmp/foo-readwrite.json <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:*"
                ],
                "Resource": [
                    "arn:aws:s3:::foo",
                    "arn:aws:s3:::foo/*"
                ]
            }
        ]
    }
    EOF

    mc admin policy create myminio foo-readonly /tmp/foo-readonly.json
    mc admin policy create myminio foo-readwrite /tmp/foo-readwrite.json

    mc admin user add myminio alfa yadayadayada
    mc admin user add myminio beta yadayadayada2

    mc admin group add myminio ad alfa
    mc admin group add myminio rer beta

    mc admin policy attach myminio foo-readwrite --group ad
    mc admin policy attach myminio foo-readonly --group rer

    echo "alfa service account. Save the output"
    mc admin user svcacct add myminio alfa --name alfasa --json
    echo "beta service account. Save the output"
    mc admin user svcacct add myminio beta --name betasa --json
    ```

#### Run
Four steps:
1. Run `docker compose -f docker-compose_dtplt_firstrun.yaml up -d`.
2. Wait for the database to be created and online. It should take seconds once the service is running. You can try connecting to it. 
3. Run `docker exec -it mini bash /var/run/secrets/minitial` and copy it's output, it has the credentials of the service account with write access and the one with read access.
4. Run `docker compose -f docker-compose_dtplt_firstrun.yaml down`. 

After this you could delete the secrets, since they won't be used by the platform.
</details>

### Running the platform
We have three compose files:
1. **docker-compose_dtpltbase.yaml** handles the database and starts the network.
2. **docker-compose_dtplt.yaml** handles airflow and the other services.

We have the database in a separate compose because one may want to restart airflow and the other services, stop them and so on more often. But we believe that the database service should be left alone as much as possible.

#### Secrets:
You will need the following files.  
In the `secrets/airflow` folder:
- `.env` with the sensible airflow envvars (or any you just prefer there).

    For instance:
    ```.env
    AIRFLOW_UID=50000
    AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
    AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
    AIRFLOW__CELERY__WORKER_CONCURRENCY=40
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
    AIRFLOW__CORE__EXECUTOR=CeleryExecutor
    AIRFLOW__CORE__FERNET_KEY=
    AIRFLOW__CORE__LOAD_EXAMPLES=false
    AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=58
    AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql:///?service=airflows
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql:///?service=airflows
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
    AIRFLOW__WEBSERVER__BASE_URL=http://localhost/baaar
    AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=true
    AIRFLOW__WEBSERVER__FILTER_BY_OWNER=true
    DUMB_INIT_SETSID=0
    PIP_ADDITIONAL_REQUIREMENTS=

    AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true
    AIRFLOW__CORE__MAX_MAP_LENGTH=40000


    AIRFLOW__CORE__DAGS_FOLDER=/home/airflow/sources/dags
    AIRFLOW__CORE__PLUGINS_FOLDER=/home/airflow/sources/plugins
    AIRFLOW__LOGGING__BASE_LOG_FOLDER=/home/airflow/sources/logs
    AIRFLOW__TEMP_STORE=/home/airflow/sources/tmp_store

    #first time
    AIRFLOW_DB_MIGRATE=true
    AIRFLOW_WWW_USER_CREATE=false
    AIRFLOW_WWW_USER_PASSWORD=bbaarr
    AIRFLOW_WWW_USER_USERNAME=ffoo
    
    #placeholder
    AF_GID=671 
    ```
To find the secret of AF_GID by running cat /etc/group and looking at the docker ID.

In the `secrets/minio` folder:
- `.envminio` has the envvars for minio. We recommend setting `MINIO_BROWSER_REDIRECT_URL`.  
   ```
   MINIO_BROWSER_REDIRECT_URL=https://foo.example.com/minio/
   ```

In the `secrets/postgres` folder:
- `dtpltpgs` contains the postgres connection service file, with the credentials to the databases.  
    We use this to handle the connections to the database, e.g. from airflow.
        For instance:
    ```.toml
    [airflows]
    host=postgres
    port=5432
    dbname=airflowdb
    user=airflowad
    password=BAR

    [ptplt]
    host=postgres
    port=5432
    dbname=dataplat
    user=dbadmn
    password=FOO
    ```

In the `secrets/grafana` folder:
- `.envgrafana` has the envvars for Grafana. We recommend setting `GF_SECURITY_ADMIN_USER` and `GF_SECURITY_ADMIN_PASSWORD`.  
   ```
  GF_SECURITY_ADMIN_USER=admingrafana
  GF_SECURITY_ADMIN_PASSWORD=BAA
   ```

#### Run
1. Run `docker compose -f docker-compose_dtpltbase.yaml up -d`.
2. Run `docker compose -f docker-compose_dtplt.yaml up -d`.

To run the sample notebooks, you must have two files in the project root directory.

The `pgser` to connect to the database:
   ```
  [ptplt]
  host=localhost
  port=25432
  dbname=dataplat
  user=dbadmn
  password=FOO
   ```
The environment `.env` variable for saving the pgser file
  ```
  PGSERVICEFILE=pgser
   ```
