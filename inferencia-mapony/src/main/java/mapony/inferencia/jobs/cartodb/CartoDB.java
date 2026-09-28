// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs.cartodb;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import mapony.inferencia.jobs.InferenciaCustomJob;
import mapony.inferencia.util.cte.CitiesCte;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.writables.CartoDb;
import mapony.inferencia.writables.RawData;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated to Spark: replaces SequenceFileInputFormat→CartoDbMap→CartoDbReducer
 * (MultipleOutputs)→CartoDbPartitioner with sc.objectFile → flatMap → per-city
 * saveAsTextFile. One output directory is created per city under rutaSalidaFicheros.
 */
public class CartoDB extends InferenciaCustomJob {

    @Override
    public void setClassLogger() {
        logger = LoggerFactory.getLogger(CartoDB.class);
    }

    @Override
    protected void init() {
        setJobName(JobNamesCte.generateCsv);
        setIndice_archivos(properties.getProperty(PropertiesCte.indice_archivo));
        setPatronFicheros(properties.getProperty(PropertiesCte.patron_ficheros));
        setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales)
                + getIndice_archivos() + getPatronFicheros());
        setRutaSalidaFicheros(properties.getProperty(PropertiesCte.salida_datos_job) + getIndice_archivos());
        setNumReducers(Integer.parseInt(properties.getProperty(PropertiesCte.reducers)));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void run() throws Exception {
        // Flatten objectFile to individual RawData records, keeping only cells with >= 20 records.
        // The >= 20 threshold mirrors the original CartoDbMap filter.
        JavaRDD<RawData> records = sc
            .<Tuple2<String, List<RawData>>>objectFile(getRutaFicheros())
            .mapToPair(t -> t)
            .filter(pair -> pair._2().size() >= 20)
            .flatMap(pair -> pair._2().iterator());

        // Write one CSV file per city. Replaces MultipleOutputs + CartoDbPartitioner.
        String[] cities = {
            CitiesCte.sLondres, CitiesCte.sBerlin, CitiesCte.sMadrid,
            CitiesCte.sRoma,    CitiesCte.sParis,  CitiesCte.sNuevaYork
        };
        for (String city : cities) {
            JavaRDD<String> cityLines = records
                .filter(rd -> city.equals(rd.getCiudad()))
                .map(rd -> new CartoDb(rd).toString());
            String cityPath = getRutaSalidaFicheros() + "/" + city;
            deleteOutputPath(cityPath);
            cityLines.saveAsTextFile(cityPath);
        }
    }

    public static void main(String[] args) throws Exception {
        checkMainClassArgs(args);
        System.exit(new CartoDB().execute(args));
    }
}
