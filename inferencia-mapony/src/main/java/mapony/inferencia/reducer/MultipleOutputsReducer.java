// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.reducer;

import java.io.IOException;
import java.util.ArrayList;

import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.pattern.reservoirsampler.ReservoirSampler;
import mapony.inferencia.writables.RawData;
import mapony.inferencia.writables.arraywritables.ArrayOfRawData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author Alvaro Sanchez Blasco
 *         Reducer comun para la ejecucion del algoritmo discriminado por ciudades (Madrid, Londres, Roma, Berlin).
 *         <p>
 *         Dependiendo del nombre de la ciudad asociado al registro(s) procesado(s) emite el resultado a un fichero
 *         asociado con la ciudad a la que pertenece
 *         <p>
 *         Se recorre la coleccion de RawData que llega como parametro, y se anyade al reservoir de tamanyo
 *         definido en las propiedades del job.
 *         <p>
 *         Al finalizar de cargar el reservoir, se emite una coleccion con las
 *         muestras seleccionadas del mismo en un ArrayWritable con clave el geohash asociado a los registros
 *         procesados.
 */
public class MultipleOutputsReducer extends Reducer<Text, RawData, Text, ArrayOfRawData> {

	private MultipleOutputs<Text, ArrayOfRawData> mos;
	private int reservoirSize;

	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, ArrayOfRawData>(context);
		reservoirSize = Integer.parseInt(context.getConfiguration().get(PropertiesCte.reservoirSize));
	}

	public void reduce(Text key, Iterable<RawData> values, Context context)
			throws IOException, InterruptedException {

		String ciudad = InferenciaCte.NO_MATTER_WHAT_CITY;
		ReservoirSampler<RawData> r = new ReservoirSampler<RawData>(reservoirSize);

		for (RawData val : values) {
			if (ciudad.equals(InferenciaCte.NO_MATTER_WHAT_CITY)) {
				ciudad = val.getCiudad().toString();
			}
			r.sample(new RawData(val));
		}

		Iterable<RawData> samplesToEmit = r.getSamples();
		ArrayList<RawData> listOfWritablesToemit = new ArrayList<RawData>();
		for (RawData sampledRawData : samplesToEmit) {
			listOfWritablesToemit.add(new RawData(sampledRawData));
		}

		mos.write(new Text(key), new ArrayOfRawData(Text.class, listOfWritablesToemit.toArray(new RawData[listOfWritablesToemit.size()])),
				ciudad);
	}

	public void cleanup(Context context) {
		try {
			mos.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}