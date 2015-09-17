// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.mapper.precission;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.inferencia.util.Position;
import mapony.inferencia.util.Utilities;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.exception.InferenciaException;
import mapony.inferencia.writables.RawData;
import mapony.inferencia.writables.arraywritables.ArrayOfRawData;

/**
 * @author Alvaro Sanchez Blasco
 *         <ul><li>[ES] Mapper del job de agrupacion de mayor precision
 *         <p>
 *         Ya no vamos a centrar el calculo del geoHash en una ciudad. Ahora, lo que haremos sera agrupar los
 *         datos segun la cercania entre ellos. Por eso recalculamos un nuevo geoHash de mayor precision (menor
 *         area) y emitimos los datos, para agruparlos.
 *         <li>[EN] 
 *         </ul>
 */
public class PrecissionMap extends Mapper<Text, ArrayOfRawData, Text, RawData> {

	private static Logger logger;
	private int geoHashPrecission;

	protected void map(Text key, ArrayOfRawData values, Context context)
			throws IOException, InterruptedException {

		try {
			Writable[] arrayOfWritable = values.get();
			// Sabemos que los datos recuperados vienen del anterior Job, por lo
			// que son validos. No tenemos que volver a validarlos para
			// procesarlos.
			for (Writable writable : arrayOfWritable) {
				RawData processedRecord = new RawData((RawData) writable);

				final String computedGeoHash = getComputedGeoHash(processedRecord);
				
				RawData rdBean = new RawData(processedRecord, computedGeoHash);

				context.write(new Text(computedGeoHash), new RawData(rdBean));
			}
		} catch (InferenciaException e) {
			logger.error(e.getMessage());
		}
	}

	private final String getComputedGeoHash(final RawData processedRecord) throws InferenciaException {
		final Position originalPosition = new Position(new Double(processedRecord.getLatitude().toString()), new Double(processedRecord
				.getLongitude().toString()));
		return Utilities.getGeoHashAsStringByPrecission(originalPosition, geoHashPrecission);
	}
	
	public void setup(Context context) {
		logger = LoggerFactory.getLogger(PrecissionMap.class);
		geoHashPrecission = Integer.parseInt(context.getConfiguration().get(PropertiesCte.precision));
	}
}