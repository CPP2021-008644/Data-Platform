FROM apache/airflow:2.9.3-python3.10
# Every modification is done as root
USER root

# Copy custom scripts
COPY ./airflow/custom_entrypoint.sh /custom_entrypoint.sh

# Update repositories and install necessary dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  gosu git nano \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && chmod +x /custom_entrypoint.sh


COPY ./airflow/requirements.txt .

USER airflow
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt 


USER root

COPY src /dataplatform/src

ENV PYTHONPATH=/dataplatform:/opt/airflow:$PYTHONPATH

# Entrypoint is substituted by our custom script
ENTRYPOINT [ "/custom_entrypoint.sh" ]
