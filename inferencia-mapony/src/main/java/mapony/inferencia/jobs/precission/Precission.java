// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs.precission;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import mapony.inferencia.jobs.InferenciaCustomJob;
import mapony.inferencia.util.Position;
import mapony.inferencia.util.Utilities;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.pattern.reservoirsampler.ReservoirSampler;
import mapony.inferencia.writables.RawData;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated to Spark: replaces SequenceFileInputFormat→PrecissionMap→CommonReducer
 * with sc.objectFile → flatMapToPair (re-hash at higher precision) → groupByKey →
 * mapValues (reservoir sampling) → saveAsObjectFile.
 */
public class Precission extends InferenciaCustomJob {

    @Override
    public void setClassLogger() {
        logger = LoggerFactory.getLogger(Precission.class);
    }

    @Override
    protected void init() {
        setJobName(JobNamesCte.precission);
        setIndice_archivos(properties.getProperty(PropertiesCte.indice_archivo));
        setPatronFicheros(properties.getProperty(PropertiesCte.patron_ficheros));
        setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales)
                + getIndice_archivos() + getPatronFicheros());
        setRutaSalidaFicheros(properties.getProperty(PropertiesCte.salida_datos_job) + getIndice_archivos());
        setNumReducers(Integer.parseInt(properties.getProperty(PropertiesCte.reducers)));
        setPrecisionGeoHash(Integer.parseInt(properties.getProperty(PropertiesCte.precision)));
        setReservoirSize(Integer.parseInt(properties.getProperty(PropertiesCte.reservoirSize)));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void run() throws Exception {
        final int precision = getPrecisionGeoHash();
        final int reservoirSz = getReservoirSize();

        // sc.objectFile() is untyped in the Java API; the cast is safe because
        // GroupNear.saveAsObjectFile() wrote Tuple2<String, List<RawData>> objects.
        JavaPairRDD<String, List<RawData>> input = sc.<Tuple2<String, List<RawData>>>objectFile(getRutaFicheros())
            .mapToPair(t -> t);

        JavaPairRDD<String, List<RawData>> result = input
            .flatMapToPair(pair -> {
                List<Tuple2<String, RawData>> out = new ArrayList<>();
                for (RawData rd : pair._2()) {
                    try {
                        Position pos = new Position(
                            Double.parseDouble(rd.getLatitude()),
                            Double.parseDouble(rd.getLongitude()));
                        String newHash = Utilities.getGeoHashAsStringByPrecission(pos, precision);
                        out.add(new Tuple2<>(newHash, new RawData(rd, newHash)));
                    } catch (Exception e) {
                        logger.debug(e.getMessage());
                    }
                }
                return out.iterator();
            })
            .groupByKey(getNumReducers())
            .mapValues(iter -> {
                ReservoirSampler<RawData> sampler = new ReservoirSampler<>(reservoirSz);
                for (RawData rd : iter) sampler.sample(new RawData(rd));
                List<RawData> out = new ArrayList<>();
                for (RawData rd : sampler.getSamples()) out.add(new RawData(rd));
                return out;
            });

        deleteOutputPath(getRutaSalidaFicheros());
        result.saveAsObjectFile(getRutaSalidaFicheros());
    }

    public static void main(String[] args) throws Exception {
        checkMainClassArgs(args);
        System.exit(new Precission().execute(args));
    }
}
