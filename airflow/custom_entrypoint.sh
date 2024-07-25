#!/bin/bash
mkdir -p $AIRFLOW__CORE__DAGS_FOLDER $AIRFLOW__CORE__PLUGINS_FOLDER $AIRFLOW__LOGGING__BASE_LOG_FOLDER
chown -R "${AIRFLOW_UID}:${AF_GID}" $AIRFLOW__CORE__DAGS_FOLDER $AIRFLOW__CORE__PLUGINS_FOLDER $AIRFLOW__LOGGING__BASE_LOG_FOLDER

chmod o+rw -R $AIRFLOW__CORE__DAGS_FOLDER

groupadd -g $AF_GID writer
usermod -aG $AF_GID airflow

if ! gosu airflow airflow db check-migrations; then
    # Initialize database
    #info "Populating database"
    gosu airflow airflow db init

else
    # Upgrade database
    #info "Upgrading database schema"
    gosu airflow airflow db migrate
    true # Avoid return false when I am not root
fi

export _AIRFLOW_DB_MIGRATE=$AIRFLOW_DB_MIGRATE
export _AIRFLOW_WWW_USER_CREATE=$AIRFLOW_WWW_USER_CREATE
export _AIRFLOW_WWW_USER_USERNAME=$AIRFLOW_WWW_USER_USERNAME
export _AIRFLOW_WWW_USER_PASSWORD=$AIRFLOW_WWW_USER_PASSWORD
export _PIP_ADDITIONAL_REQUIREMENTS=$PIP_ADDITIONAL_REQUIREMENTS

gosu airflow /entrypoint $AIRFLOW_COMMAND