// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.mapper.groupNear;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.inferencia.util.Position;
import mapony.inferencia.util.Utilities;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.exception.InferenciaException;
import mapony.inferencia.util.validation.ComplexValidation;
import mapony.inferencia.writables.RawData;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Mapper para el Job <code>GroupNear</code>. Recibe una linea de texto, y la parsea a un CustomWritable. Emite como clave el GeoHash
 *         calculado con la precision que se ha definido para el job, y el CustomWritable generado.
 */
public class GroupNearMap extends Mapper<LongWritable, Text, Text, RawData> {

	private static Logger logger;
	private int geoHashPrecission;

	protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

		String[] dato = line.toString().split(InferenciaCte.TAB);
		try {
			if (ComplexValidation.isRecordUseful(dato)) {

				final String calculatedGeoHash = getComputedGeoHash(dato);

				RawData rdBean = new RawData(dato, calculatedGeoHash);

				context.write(new Text(calculatedGeoHash), new RawData(rdBean));
			}
		} catch (InferenciaException e) {
			logger.error(e.getMessage());
		}
	}

	private final String getComputedGeoHash(final String[] dato) throws InferenciaException {
		final Position originalPosition = new Position(dato);
		return Utilities.getGeoHashAsStringByPrecission(originalPosition, geoHashPrecission);
	}

	public void setup(Context context) {
		logger = LoggerFactory.getLogger(GroupNearMap.class);
		geoHashPrecission = Integer.parseInt(context.getConfiguration().get(PropertiesCte.precision));
	}
}
