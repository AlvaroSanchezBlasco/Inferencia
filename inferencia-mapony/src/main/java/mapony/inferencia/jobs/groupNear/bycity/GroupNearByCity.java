// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs.groupNear.bycity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import mapony.inferencia.geoHash.bean.GeoHashBean;
import mapony.inferencia.geoHash.util.GeoHashCiudad;
import mapony.inferencia.jobs.groupNear.GroupNear;
import mapony.inferencia.util.Position;
import mapony.inferencia.util.Utilities;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.pattern.reservoirsampler.ReservoirSampler;
import mapony.inferencia.util.validation.ComplexValidation;
import mapony.inferencia.writables.RawData;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated to Spark: restricts GroupNear to the six target cities by filtering
 * records whose GeoHash matches a known city centre hash.
 * GeoHashBean is not needed in the lambda — we extract a HashMap<String,String>
 * (geoHash → cityName) before the lambda to avoid serialization constraints.
 */
public class GroupNearByCity extends GroupNear {

    @Override
    public void setClassLogger() {
        logger = LoggerFactory.getLogger(GroupNearByCity.class);
    }

    @Override
    protected void init() {
        setJobName(JobNamesCte.groupNearByCity);
        setIndiceArchivo(properties.getProperty(PropertiesCte.indice_archivo));
        setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales)
                + getIndiceArchivo() + PropertiesCte.ext_archivos);
        setNumReducers(Integer.parseInt(properties.getProperty(PropertiesCte.reducers)));
        setPrecisionGeoHash(Integer.parseInt(properties.getProperty(PropertiesCte.precision)));
        setReservoirSize(Integer.parseInt(properties.getProperty(PropertiesCte.reservoirSize)));
        setRutaSalidaFicheros(properties.getProperty(PropertiesCte.salida_datos_job) + getIndiceArchivo());
    }

    @Override
    protected void run() throws Exception {
        final int precision = getPrecisionGeoHash();
        final int reservoirSz = getReservoirSize();

        // Extract String-only map so the lambda captures only Serializable types.
        HashMap<String, GeoHashBean> selectedCities = GeoHashCiudad.loadSelectedCities(precision);
        final Map<String, String> cityByHash = new HashMap<>();
        for (Map.Entry<String, GeoHashBean> e : selectedCities.entrySet()) {
            cityByHash.put(e.getKey(), e.getValue().getCity());
        }

        JavaPairRDD<String, List<RawData>> result = sc.textFile(getRutaFicheros())
            .flatMapToPair(line -> {
                String[] dato = line.split(InferenciaCte.TAB);
                try {
                    if (ComplexValidation.isRecordUseful(dato)) {
                        Position pos = new Position(dato);
                        String geoHash = Utilities.getGeoHashAsStringByPrecission(pos, precision);
                        if (cityByHash.containsKey(geoHash)) {
                            String city = cityByHash.get(geoHash);
                            return Collections.singletonList(
                                new Tuple2<>(geoHash, new RawData(dato, geoHash, city))).iterator();
                        }
                    }
                } catch (Exception e) {
                    logger.debug(e.getMessage());
                }
                return Collections.emptyIterator();
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
        System.exit(new GroupNearByCity().execute(args));
    }
}
