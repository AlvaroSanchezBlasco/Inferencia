// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;

import mapony.inferencia.util.InferenciaMessages;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.exception.InferenciaException;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated from Hadoop Tool/Configured to a plain abstract class backed by
 * JavaSparkContext. The template-method structure (init → preRun → run) mirrors
 * the original Hadoop lifecycle so each job subclass changes as little as possible.
 */
public abstract class InferenciaCustomJob {

    protected static Properties properties;
    protected static Logger logger;
    protected JavaSparkContext sc;

    private String rutaFicheros;
    private int numReducers;
    private int precisionGeoHash;
    private int reservoirSize;
    private String indiceArchivo;
    private String rutaSalidaFicheros;
    private String rutaPaises;
    private String jobName;
    private String indice_archivos;
    private String patronFicheros;

    /** Entry point called by Driver. */
    public final int execute(String[] args) {
        try {
            loadProperties(args[0]);
            setClassLogger();
            init();
            logger.info(InferenciaMessages.jobBegins(getJobName()));
            preRun();
            sc = createSparkContext();
            run();
            logger.info(InferenciaMessages.jobEndedSuccessful(getJobName()));
            return InferenciaCte.SUCCESS;
        } catch (Exception e) {
            if (logger != null) logger.error(e.getMessage(), e);
            return InferenciaCte.JOB_COMPLETION_FAILED;
        } finally {
            if (sc != null) sc.close();
        }
    }

    /**
     * Creates the JavaSparkContext. Subclasses that need extra SparkConf settings
     * (e.g., Load sets es.nodes here) should override this method.
     */
    protected JavaSparkContext createSparkContext() {
        SparkConf conf = new SparkConf().setAppName(getJobName());
        return new JavaSparkContext(conf);
    }

    /** Called after init() but before the Spark context is created. Override for pre-Spark setup (e.g. ES index creation). */
    protected void preRun() throws Exception {}

    public abstract void setClassLogger();

    /** Read properties and populate job-specific fields (rutaFicheros, jobName, etc.). */
    protected abstract void init();

    /** The actual Spark pipeline logic. sc is ready when this is called. */
    protected abstract void run() throws Exception;

    protected static void checkMainClassArgs(final String args[]) {
        if (args.length != InferenciaCte.NUM_ARGS) {
            System.out.println(PropertiesCte.usage);
            System.exit(InferenciaCte.FAIL);
        }
    }

    protected void loadProperties(final String fileName) throws InferenciaException {
        if (null == properties) {
            properties = new Properties();
        }
        try {
            FileInputStream in = new FileInputStream(fileName);
            properties.load(in);
        } catch (FileNotFoundException e) {
            throw new InferenciaException(e, e.getMessage());
        } catch (IOException e) {
            throw new InferenciaException(e, e.getMessage());
        }
        if (logger != null) logger.info(PropertiesCte.PROPERTIES_LOADED);
    }

    /**
     * Returns the HDFS NameNode URI from properties (hdfs_uri key) with fallback
     * to InferenciaCte.hdfsUri so deployments to other clusters need no code change.
     */
    protected String getHdfsUri() {
        String uri = properties.getProperty(PropertiesCte.hdfs_uri);
        return (uri != null && !uri.isEmpty()) ? uri : InferenciaCte.hdfsUri;
    }

    /** Deletes the HDFS output path before writing, so jobs can be re-run safely. */
    protected void deleteOutputPath(String pathStr) throws Exception {
        Path p = new Path(pathStr);
        FileSystem fs = p.getFileSystem(new Configuration());
        if (fs.exists(p)) {
            fs.delete(p, true);
        }
    }

    protected int noDataFound() {
        logger.error(InferenciaMessages.noDataInSpecifiedPath(getRutaFicheros()));
        return InferenciaCte.FAIL;
    }

    protected final String getIndiceArchivo() { return indiceArchivo; }
    protected final void setIndiceArchivo(String indiceArchivo) { this.indiceArchivo = indiceArchivo; }
    protected final String getRutaFicheros() { return rutaFicheros; }
    protected final void setRutaFicheros(String rutaFicheros) { this.rutaFicheros = rutaFicheros; }
    protected final int getNumReducers() { return numReducers; }
    protected final void setNumReducers(int numReducers) { this.numReducers = numReducers; }
    protected final int getPrecisionGeoHash() { return precisionGeoHash; }
    protected final void setPrecisionGeoHash(int precisionGeoHash) { this.precisionGeoHash = precisionGeoHash; }
    protected final int getReservoirSize() { return reservoirSize; }
    protected final void setReservoirSize(int reservoirSize) { this.reservoirSize = reservoirSize; }
    protected final String getRutaSalidaFicheros() { return rutaSalidaFicheros; }
    protected final void setRutaSalidaFicheros(String rutaSalidaFicheros) { this.rutaSalidaFicheros = rutaSalidaFicheros; }
    protected final String getRutaPaises() { return rutaPaises; }
    protected final void setRutaPaises(String rutaPaises) { this.rutaPaises = rutaPaises; }
    protected final String getJobName() { return jobName; }
    protected final void setJobName(String jobName) { this.jobName = jobName; }
    protected final String getIndice_archivos() { return indice_archivos; }
    protected final void setIndice_archivos(String indice_archivos) { this.indice_archivos = indice_archivos; }
    protected final String getPatronFicheros() { return patronFicheros; }
    protected final void setPatronFicheros(String patronFicheros) { this.patronFicheros = patronFicheros; }
}
