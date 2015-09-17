// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import mapony.inferencia.writables.CartoDb;
import mapony.inferencia.writables.RawData;

/**
 * @author Alvaro Sanchez Blasco
 *         Reducer para cargar datos en CartoDB
 *         <p>
 *         Se recorre la coleccion de CartoDb que llega como parametro. Se emiten ficheros por cada ciudad en formato para carga en CartoDb.
 */
public class CartoDbReducer extends Reducer<Text, RawData, Text, Text> {

	private MultipleOutputs<Text, Text> mos;
	
	public void reduce(Text key, Iterable<RawData> values, Context context)
			throws IOException, InterruptedException {
		//Creamos un reservoir de tamanyo definido en las propiedades del job.
		for (RawData rd : values) {
			CartoDb cdb = new CartoDb(rd);
			mos.write(new Text(cdb.getIdentifier()), new Text(cdb.toString()),cdb.getCiudad().toString());
		}
	}

	public void setup(Context context) {
		mos = new MultipleOutputs<Text, Text>(context);
	}
}