<img src="https://afs-services.com/wp-content/uploads/2025/09/dp_banner.png" alt="RV Banner" style="width: 100%; height: auto; display: block;">

## 
A modular data platform for **time-series storage** and **operational analytics**. It combines **PostgreSQL + TimescaleDB** for scalable storage, **Apache Airflow** for scheduled ingestion/backfills, **Grafana** for dashboards, and lightweight **Python utilities** for fast reads/writes.

> Looking for setup instructions? See **[docs/deploy.md](docs/deploy.md)**.

## Why
- Store and query market and climate time-series efficiently (hypertables, compression).
- Automate ingestion and backfills with Airflow DAGs.
- Expose curated dashboards out-of-the-box with Grafana provisioning.
- Use simple Python helpers to move data in/out at high throughput.

## Key components
- **Storage**: PostgreSQL + TimescaleDB (hypertables, compression, retention).
- **Orchestration**: Apache Airflow (sample DAGs included).
- **Visualization**: Grafana with pre-provisioned datasources/dashboards.
- **Python utilities**: `src/` helpers (e.g., fast inserts, async fetch).


### Minimal Python Example
```python 
from src.tslibv2 import TSLib
ts = TSLib(connection_params=...)  # e.g., uses a PGSERVICEFILE
ts.createdbifneeded("prices_tick", schema=...)
ts.insert_remote("prices_tick", df)  # bulk insert a pandas DataFrame
```
### Repository Layout:
``` bash 
airflow/                # image, entrypoint, DAGs
dashboards/             # Grafana JSON dashboards
pg/                     # TimescaleDB init/tuning/backfill scripts
provisioning/           # Grafana datasource/dashboard provisioning
src/                    # Python utilities (TSLib v2, saver/loader)
docker-compose_*.yaml
docs/                   # Deployment and architecture docs

```
## Acknowledgements

This work has been supported by the Government of Spain (Ministerio de Ciencia e Innovación) and the European Union through Project CPP2021-008644 / AEI / 10.13039/501100011033 / Unión Europea Next GenerationEU / PRTR. Visit our website for more information [Green and Digital Finance – Next GenerationEU](https://afs-services.com/proyectos-nextgen-eu/).

<p align="center">
  <img
    src="https://afs-services.com/wp-content/uploads/2025/06/logomciaienetgeneration-1232x264.png"
    alt="Logo MCIAI NetGeneration"
    height="100"
  >
</p>