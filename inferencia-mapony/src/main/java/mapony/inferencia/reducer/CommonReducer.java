// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.reducer;

import java.io.IOException;
import java.util.ArrayList;

import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.pattern.reservoirsampler.ReservoirSampler;
import mapony.inferencia.writables.RawData;
import mapony.inferencia.writables.arraywritables.ArrayOfRawData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Alvaro Sanchez Blasco
 *         Reducer comun para la ejecucion del agrupacion.
 *         <p>
 *         Se recorre la coleccion de RawData que llega como parametro, y se anyade al reservoir de tamanyo
 *         definido en las propiedades del job.
 *         <p>
 *         Al finalizar de cargar el reservoir, se emite una coleccion con las
 *         muestras seleccionadas del mismo en un ArrayWritable con clave el geohash asociado a los registros
 *         procesados.
 */
public class CommonReducer extends Reducer<Text, RawData, Text, ArrayOfRawData> {

	private int reservoirSize;

	public void reduce(Text key, Iterable<RawData> values, Context context)
			throws IOException, InterruptedException {
		//Creamos un reservoir de tamanyo definido en las propiedades del job.
		ReservoirSampler<RawData> reservoir = new ReservoirSampler<RawData>(reservoirSize);
		for (RawData RawData : values) {
			reservoir.sample(new RawData(RawData));
		}
		
		Iterable<RawData> samplesToEmit = reservoir.getSamples();

		ArrayList<RawData> listOfWritablesToEmit = new ArrayList<RawData>();

		for (RawData sampledRawData : samplesToEmit) {
			listOfWritablesToEmit.add(new RawData(sampledRawData));
		}
		
		context.write(new Text(key),
				new ArrayOfRawData(Text.class, listOfWritablesToEmit.toArray(new RawData[listOfWritablesToEmit.size()])));
	}

	public void setup(Context context) {
		reservoirSize = Integer.parseInt(context.getConfiguration().get(PropertiesCte.reservoirSize));
	}
}