#!/bin/bash

create_sql=`mktemp`

# Checks to support bitnami image with same scripts so they stay in sync
if [ ! -z "${BITNAMI_APP_NAME:-}" ]; then
	if [ -z "${POSTGRES_USER:-}" ]; then
		POSTGRES_USER=${POSTGRESQL_USERNAME}
	fi

	if [ -z "${POSTGRES_DB:-}" ]; then
		POSTGRES_DB=${POSTGRESQL_DATABASE}
	fi

	if [ -z "${PGDATA:-}" ]; then
		PGDATA=${POSTGRESQL_DATA_DIR}
	fi
fi

if [ -z "${POSTGRESQL_CONF_DIR:-}" ]; then
	POSTGRESQL_CONF_DIR=${PGDATA}
fi

export PGPASSWORD="$POSTGRESQL_PASSWORD"

psql -U "${POSTGRES_USER}" "${POSTGRES_DB}" -f /tmp/base
psql -U "${POSTGRES_USER}" "${POSTGRES_DB}" -f /tmp/basedata
psql -U "${POSTGRES_USER}" "${POSTGRES_DB}" -f /tmp/backfill
psql -U "${POSTGRES_USER}" "${POSTGRES_DB}" -f /var/run/secrets/airflow
psql -U "${POSTGRES_USER}" "${POSTGRES_DB}" -f /var/run/secrets/auth
