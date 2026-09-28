# Inferencia de lugares icónicos

Hadoop MapReduce pipeline that processes Flickr geotagged photo data to infer iconic
locations in a set of target cities, and loads results into ElasticSearch and CartoDB.

Result published at: http://cdb.io/1KkxPTi

---

## Architecture

The pipeline is a chain of MapReduce jobs. Each job reads from HDFS and writes back to
HDFS. Jobs are packaged into a single fat JAR and invoked by name through the `Driver`
entry point.

```
Flickr TSV dataset (HDFS)
        │
        ▼
  1. groupNear / groupNearCities
        │  Groups geotagged photos by GeoHash cell.
        │  Output: SequenceFile (Text geohash → ArrayOfRawData), BZip2-compressed.
        ▼
  2. precission / precissionCities
        │  Re-groups at higher GeoHash precision (more accurate cells).
        │  Output: SequenceFile (Text geohash → ArrayOfRawData), BZip2-compressed.
        ▼
  3. load
        │  Reads SequenceFiles and indexes each record into ElasticSearch.
        │  Output: ElasticSearch index (configured via properties file).
        ▼
  4. cartodb  (optional)
        │  Reads SequenceFiles and emits one CSV row per record, partitioned by city.
        │  Output: Text CSV files ready for CartoDB bulk ingestion.
        ▼
    CartoDB / ElasticSearch
```

The `*ByCity` variants (`groupNearCities`, `precissionCities`) restrict processing to
six predefined cities: Madrid, London, Berlin, Rome, Paris, New York.

---

## Building

```
cd inferencia-mapony
mvn package
```

Produces `target/inferencia-mapony-1.0.0-ejecucion.jar`.

---

## Running

All jobs share the same invocation pattern:

```
hadoop jar inferencia-mapony-1.0.0-ejecucion.jar <job-name> <config.properties>
```

### Job names

| Name               | Description                                      |
|--------------------|--------------------------------------------------|
| `groupNear`        | Step 1 — group all records by GeoHash            |
| `groupNearCities`  | Step 1 (cities only) — group by GeoHash per city |
| `precission`       | Step 2 — re-group at higher GeoHash precision    |
| `precissionCities` | Step 2 (cities only)                             |
| `load`             | Step 3 — index results into ElasticSearch        |
| `cartodb`          | Optional — generate CartoDB CSV files            |

### Typical full-pipeline execution

```bash
hadoop jar inferencia-mapony-1.0.0-ejecucion.jar groupNear      config.properties
hadoop jar inferencia-mapony-1.0.0-ejecucion.jar precission     config.properties
hadoop jar inferencia-mapony-1.0.0-ejecucion.jar load           config.properties

# Optional CartoDB export (run after precission):
hadoop jar inferencia-mapony-1.0.0-ejecucion.jar cartodb        config.properties
```

---

## Properties file

Create a `config.properties` file and pass it as the last argument to every job.
All keys are required unless marked optional.

| Key                  | Description                                                                  |
|----------------------|------------------------------------------------------------------------------|
| `ruta_inicial_fichero` | HDFS path prefix for input files (e.g. `hdfs:///data/flickr/`)             |
| `ruta_salida_job`      | HDFS path prefix for output directories                                    |
| `ruta_paises`          | HDFS path to the GeoNames countries dataset (used by GroupNear)            |
| `indice_archivo`       | Identifier appended to input/output paths to distinguish dataset batches   |
| `patron_ficheros`      | Glob pattern appended to input path for Precission/CartoDB (e.g. `/part-r-*`) |
| `numero_reducer`       | Number of Reduce tasks                                                     |
| `precision`            | GeoHash precision level (integer 1–12; see InferenciaCte for cell sizes)   |
| `reservoir`            | Reservoir sampler size — max records kept per GeoHash cell                 |
| `hdfs_uri`             | *(optional)* HDFS NameNode URI. Defaults to `hdfs://quickstart.cloudera:8020/` |

### ElasticSearch properties (required for `load` job only)

| Key                   | Description                            |
|-----------------------|----------------------------------------|
| `es_index_name`       | ElasticSearch index name               |
| `es_type_name`        | ElasticSearch type name                |
| `es_cluster_name`     | ElasticSearch cluster name             |
| `es_cluster_ip`       | ElasticSearch node IP                  |
| `es_cluster_port`     | ElasticSearch node port                |

### CartoDB partitioner note

The `cartodb` job distributes records across **6 reducers** (one per city). Set
`numero_reducer=6` in the properties file when running `cartodb`.

---

## Input dataset format

Tab-separated values (TSV). The pipeline reads the following columns by index:

| Index | Field          |
|-------|----------------|
| 0     | identifier     |
| 3     | date taken     |
| 5     | capture device |
| 6     | title          |
| 7     | description    |
| 8     | user tags      |
| 9     | machine tags   |
| 10    | longitude      |
| 11    | latitude       |
| 14    | download URL   |

---

## Dependencies

| Library                      | Version | Scope    |
|------------------------------|---------|----------|
| Apache Hadoop Common         | 2.7.1   | provided |
| Hadoop MapReduce Client      | 2.7.1   | provided |
| Hadoop HDFS                  | 2.7.1   | provided |
| elasticsearch-hadoop-mr      | 2.1.0   | compile  |
| elasticsearch                | 1.7.0   | compile  |
| geohash (ch.hsr)             | 1.0.13  | compile  |
