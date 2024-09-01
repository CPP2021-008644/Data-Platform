# EU Taxonomy — Minimal Schema and ETL

## Objective

Ingest the Excel files from the **EU Taxonomy Compass** into PostgreSQL, normalize them to a stable schema, and expose an **eligibility by NACE** view (matching, not alignment).

## Logical Diagram (ASCII)

```
[release]
   id PK
   name UNIQUE
   source_url
   fetched_at
          1
          |
          *---- [taxonomy_objective]
                            id PK
                            code
                            name
                            release_id FK
                                     1
                                     |
                                     *---- [taxonomy_activity]
                                                   id PK
                                                   compass_id UNIQUE per release
                                                   title
                                                   description
                                                   objective_id FK
                                                   release_id FK
                                                            |
                                                            *---- [activity_nace] (activity_id, nace_code) PK
                                                            |
                                                            *---- [criteria_sc] (id, activity_id, criterion_text, threshold_value, threshold_unit, legal_ref)
                                                            |
                                                            *---- [criteria_dnsh] (id, activity_id, criterion_text, legal_ref)
                                                            |
                                                            *---- [criteria_ms] (id, activity_id, reference)
                                                            |
                                                            *---- [legal_reference] (id, activity_id, act_name, article, annex, url)
```

## ETL Flow

1. Load the file using `--source {csv,xlsx,json}` with `pandas`.
2. Normalize columns to:
    - `objective_code`, `objective_name`
    - `activity_compass_id`, `activity_title`, `activity_description`
    - `nace_codes` (separated by `;` or `,`)
    - `sc_criteria`, `dnsh_criteria`, `ms_references` (lists separated by `;`)
    - `legal_act`, `legal_article`, `legal_annex`, `legal_url` (multiple values separated by `;`)
3. UPSERT:
    - `release` by `name`
    - `taxonomy_objective` by `(release_id, code)`
    - `taxonomy_activity` by `(release_id, compass_id)`
    - The rest with `ON CONFLICT DO NOTHING` plus logical keys to avoid duplicates.

## View `eligibility_by_nace`

Returns, for each `nace_code` and activity, arrays of **SC** and **DNSH** criteria IDs and their counts:

```sql
SELECT * FROM eligibility_by_nace WHERE nace_code = 'D35.11';
```

Columns:
- `nace_code`
- `activity_id`
- `title`
- `sc_criteria_ids` (int[])
- `sc_count` (int)
- `dnsh_criteria_ids` (int[])
- `dnsh_count` (int)

> The view **does not** certify *alignment*. It is a matching and a basic **checklist** for internal analysis.

## Reproducible Execution

1. `psql -f sql/taxonomy/schema.sql`
2. `python etl/taxonomy/ingest_taxonomy_compass.py --source csv --path data/external/taxonomy_compass/fixtures/compass_min.csv --release-name Compass_min --source-url fixtures://compass_min --database-url $DATABASE_URL`
3. `psql -f sql/taxonomy/views.sql`
4. Query `eligibility_by_nace`.

## Attribution

Public data from the European Commission (EU Taxonomy Compass). See `LICENSE-NOTE-TAXONOMY.md`.
