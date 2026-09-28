// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs.load;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import mapony.inferencia.jobs.InferenciaCustomJob;
import mapony.inferencia.util.InferenciaMessages;
import mapony.inferencia.util.Utilities;
import mapony.inferencia.util.cte.ElasticSearchClusterCte;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.JsonCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.elasticsearchclient.ElasticSearchClient;
import mapony.inferencia.util.exception.InferenciaException;
import mapony.inferencia.util.validation.ComplexValidation;
import mapony.inferencia.writables.RawData;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated to Spark: replaces SequenceFileInputFormat→LoadMap→EsOutputFormat
 * with sc.objectFile → flatMap → JavaEsSpark.saveToEs().
 * ElasticSearchClient (index setup) runs in preRun() before the Spark context
 * is created so it uses the standalone ES transport client, not the Spark connector.
 */
public class Load extends InferenciaCustomJob {

    private String clusterIp;
    private String clusterPort;
    private static String indexClusterES;
    private static String typeClusterES;
    private static String clusterName;

    @Override
    public void setClassLogger() {
        logger = LoggerFactory.getLogger(Load.class);
    }

    @Override
    protected void loadProperties(String fileName) throws InferenciaException {
        super.loadProperties(fileName);
        indexClusterES = properties.getProperty(ElasticSearchClusterCte.indexName);
        typeClusterES  = properties.getProperty(ElasticSearchClusterCte.typeName);
        clusterName    = properties.getProperty(ElasticSearchClusterCte.clusterName);
    }

    @Override
    protected void init() {
        setJobName(JobNamesCte.loadInElasticSearch);
        setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales));
        clusterIp   = properties.getProperty(ElasticSearchClusterCte.ip);
        clusterPort = properties.getProperty(ElasticSearchClusterCte.port);
    }

    /** Creates the ES index (drops and recreates if it exists) before starting Spark. */
    @Override
    protected void preRun() {
        final String[] params = { indexClusterES, typeClusterES, clusterName };
        new ElasticSearchClient(params);
        logger.info(InferenciaMessages.connectedToElasticSearchCluster());
    }

    /** Sets ES connection properties on SparkConf so the ES-Spark connector can find the cluster. */
    @Override
    protected JavaSparkContext createSparkContext() {
        SparkConf conf = new SparkConf()
            .setAppName(getJobName())
            .set("es.nodes",       clusterIp + ":" + clusterPort)
            .set("es.mapping.id",  JsonCte.idObject)
            .set("es.nodes.wan.only", "true");
        return new JavaSparkContext(conf);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void run() throws Exception {
        // Read objectFile written by Precission (Tuple2<String geoHash, List<RawData>>)
        JavaRDD<Map<String, Object>> docs = sc
            .<Tuple2<String, List<RawData>>>objectFile(getRutaFicheros())
            .mapToPair(t -> t)
            // Discard geo-hash cells with fewer than 15 records (not statistically significant)
            .filter(pair -> pair._2().size() >= 15)
            .flatMap(pair -> pair._2().iterator())
            .flatMap(rd -> {
                try {
                    if (ComplexValidation.isCustomWritableUseful(rd)) {
                        String date = Utilities.getElasticSearchDateFieldFromString(rd.getDateTaken());
                        Map<String, Object> doc = new LinkedHashMap<>();
                        doc.put(JsonCte.idObject,            rd.getIdentifier());
                        doc.put(JsonCte.tituloObject,        Utilities.cleanString(rd.getTitle()));
                        doc.put(JsonCte.descripcionObject,   Utilities.cleanString(rd.getDescription()));
                        doc.put(JsonCte.userTagsObject,      Utilities.cleanString(rd.getUserTags()));
                        doc.put(JsonCte.machineTagsObject,   Utilities.cleanString(rd.getMachineTags()));
                        doc.put(JsonCte.locationObject,      rd.getLatitude() + InferenciaCte.COMMA + rd.getLongitude());
                        doc.put(JsonCte.fotoObject,          rd.getDownloadUrl());
                        doc.put(JsonCte.captureDeviceObject, Utilities.cleanString(rd.getCaptureDevice()));
                        doc.put(JsonCte.fechaCapturaObject,  date);
                        doc.put(JsonCte.ciudadObject,        rd.getCiudad());
                        return Collections.singletonList(doc).iterator();
                    }
                } catch (Exception e) {
                    logger.debug(e.getMessage());
                }
                return Collections.emptyIterator();
            });

        JavaEsSpark.saveToEs(docs, indexClusterES + "/" + typeClusterES);
    }

    public static void main(String[] args) throws Exception {
        checkMainClassArgs(args);
        System.exit(new Load().execute(args));
    }
}
