package mapony.inferencia.c_carga.mapper;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.util.clases.MaponyUtil;
import mapony.util.constantes.MaponyCte;
import mapony.util.constantes.MaponyJsonCte;
import mapony.util.writables.RawDataWritable;
import mapony.util.writables.array.RawDataArrayWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Mapper del proceso de carga de datos en ElasticSearch.
 *         <p>
 *         Recibimos pares clave (geoHash) valor (RawDataArrayWritable)
 *         <p>
 *         Por cada elemento del RawDataArrayWritable, creamos un MapWritable que contendrá pares de clave valor, con
 *         clave identificador el campo del json que queremos cargar, y valor, el valor asociado, para su procesado por
 *         los Reducer de ElasticSearch.
 */
public class MaponyCargaESMap extends Mapper<Text, RawDataArrayWritable, Text, MapWritable> {

	private final Logger logger = LoggerFactory.getLogger(MaponyCargaESMap.class);

	protected void map(Text geoHash, RawDataArrayWritable values, Context context)
			throws IOException, InterruptedException {

		Writable[] arrValues = values.get();
		// Descartamos aquellas colecciones de valores que superen más de 30 elementos, de tal forma que evitamos puntos
		// no significativos.
		if (arrValues.length >= 30) {
			for (Writable writable : arrValues) {
				/**
				 * @param identifier[0]
				 * @param dateTaken[1]
				 * @param captureDevice[2]
				 * @param title[3]
				 * @param description[4]
				 * @param userTags[5]
				 * @param machineTags[6]
				 * @param longitude[7]
				 * @param latitude[8]
				 * @param downloadUrl[9]
				 * @param geoHash[10]
				 * @param continente[11]
				 * @param pais[12]
				 * @param ciudad[13]
				 */
				RawDataWritable t = new RawDataWritable((RawDataWritable) writable);
				Text key = new Text(t.getIdentifier());
				String date = MaponyUtil.getFechaFromString(t.getDateTaken().toString());
				// // Unicamente insertamos en ES si el campo fecha viene informado.
				if (MaponyUtil.stringTieneValor(date)) {
					try {

						MapWritable mwr = new MapWritable();

						mwr.put(new Text(MaponyJsonCte.idObject), new Text(key));

						mwr.put(new Text(MaponyJsonCte.tituloObject),
								new Text(MaponyUtil.cleanString(t.getTitle().toString())));

						mwr.put(new Text(MaponyJsonCte.descripcionObject),
								new Text(MaponyUtil.cleanString(t.getDescription().toString())));

						mwr.put(new Text(MaponyJsonCte.userTagsObject),
								new Text(MaponyUtil.cleanString(t.getUserTags().toString())));

						mwr.put(new Text(MaponyJsonCte.machineTagsObject),
								new Text(MaponyUtil.cleanString(t.getMachineTags().toString())));

						mwr.put(new Text(MaponyJsonCte.locationObject),
								new Text(t.getLatitude().toString() + MaponyCte.COMA + t.getLongitude().toString()));

						mwr.put(new Text(MaponyJsonCte.fotoObject), new Text(t.getDownloadUrl().toString()));

						mwr.put(new Text(MaponyJsonCte.captureDeviceObject),
								new Text(MaponyUtil.cleanString(t.getCaptureDevice().toString())));

						mwr.put(new Text(MaponyJsonCte.fechaCapturaObject), new Text(date));

						mwr.put(new Text(MaponyJsonCte.ciudadObject), new Text(t.getCiudad().toString()));

						context.write(new Text(key), new MapWritable(mwr));
					} catch (Exception e) {
						getLogger().error(e.getMessage());
						e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 * @return the logger
	 */
	private final Logger getLogger() {
		return logger;
	};
}
