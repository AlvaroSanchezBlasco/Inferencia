// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.mapper.load;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.inferencia.util.Utilities;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.JsonCte;
import mapony.inferencia.util.exception.InferenciaException;
import mapony.inferencia.util.validation.ComplexValidation;
import mapony.inferencia.writables.RawData;
import mapony.inferencia.writables.arraywritables.ArrayOfRawData;

/**
 * @author Alvaro Sanchez Blasco
 *         <ul>
 *         <li>[ES] Mapper del proceso de carga de datos en ElasticSearch.
 *         <p>
 *         Recibimos pares {clave (geoHash), valor (ArrayOfRawData)}
 *         <p>
 *         Por cada elemento del ArrayOfRawData, creamos un MapWritable que contendra pares de clave valor, con clave identificador el campo del json
 *         que queremos cargar, y valor, el valor asociado, para su procesado por los Reducer de ElasticSearch.
 *         <li>[EN] Mapper of the Load Job into ElasticSearch.
 *         <p>
 *         Recieves pairs {key (geohash), value (ArrayOfRawData)}
 *         <p>
 *         For each element on the ArrayOfRawData, we'll create a MapWritable instance that will contain pairs key/value. The key will be the field of
 *         the JSon that we want to load, and the value, the data of the referenced field.
 *         </ul>
 */
public class LoadMap extends Mapper<Text, ArrayOfRawData, Text, MapWritable> {

	private static Logger logger = LoggerFactory.getLogger(LoadMap.class);
	
	protected void map(Text geoHash, ArrayOfRawData values, Context context) throws IOException, InterruptedException {

		Writable[] arrValues = values.get();
		// [ES] Descartamos aquellas colecciones de valores que superen mas de 30 elementos, de tal forma que evitamos
		// agrupaciones de datos no significativas.
		// [EN] We don't evaluate data[] with less than 15 elements, so we avoid irrelevant data groupings.
		if (arrValues.length >= 15) {

			for (Writable writable : arrValues) {

				RawData recordToLoadInES = new RawData((RawData) writable);
				Text key = new Text(recordToLoadInES.getIdentifier());
				
				// // Unicamente insertamos en ES si el campo fecha viene informado.
				try {
					if (ComplexValidation.isCustomWritableUseful(recordToLoadInES)) {

						final String date = Utilities.getElasticSearchDateFieldFromString(recordToLoadInES.getDateTaken().toString());
						
						MapWritable mapWritableToEmit = new MapWritable();

						mapWritableToEmit.put(new Text(JsonCte.idObject), new Text(key));

						mapWritableToEmit.put(new Text(JsonCte.tituloObject), new Text(Utilities.cleanString(recordToLoadInES.getTitle().toString())));

						mapWritableToEmit.put(new Text(JsonCte.descripcionObject), new Text(Utilities.cleanString(recordToLoadInES.getDescription().toString())));

						mapWritableToEmit.put(new Text(JsonCte.userTagsObject), new Text(Utilities.cleanString(recordToLoadInES.getUserTags().toString())));

						mapWritableToEmit.put(new Text(JsonCte.machineTagsObject), new Text(Utilities.cleanString(recordToLoadInES.getMachineTags().toString())));

						mapWritableToEmit.put(new Text(JsonCte.locationObject), new Text(recordToLoadInES.getLatitude().toString() + InferenciaCte.COMMA
								+ recordToLoadInES.getLongitude().toString()));

						mapWritableToEmit.put(new Text(JsonCte.fotoObject), new Text(recordToLoadInES.getDownloadUrl().toString()));

						mapWritableToEmit.put(new Text(JsonCte.captureDeviceObject),
								new Text(Utilities.cleanString(recordToLoadInES.getCaptureDevice().toString())));

						mapWritableToEmit.put(new Text(JsonCte.fechaCapturaObject), new Text(date));

						mapWritableToEmit.put(new Text(JsonCte.ciudadObject), new Text(recordToLoadInES.getCiudad().toString()));

						context.write(new Text(key), new MapWritable(mapWritableToEmit));

					}
				} catch (InferenciaException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}
}
