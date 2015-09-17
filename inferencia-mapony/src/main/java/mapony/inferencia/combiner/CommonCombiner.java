// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.combiner;

import java.io.IOException;

import mapony.inferencia.writables.RawData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Alvaro Sanchez Blasco
 *         <ul>
 *         <li>[ES] Clase de combiner comun a todos los jobs que la requieran. Combina los datos antes de mandarlos a los reducer.
 *         <li>[EN] Common combiner class. Combines the records before send them to the Reducer.
 *         </ul>
 */
public class CommonCombiner extends Reducer<Text, RawData, Text, RawData> {

	public void reduce(Text key, Iterable<RawData> values, Context context)
			throws IOException, InterruptedException {
		for (RawData value : values) {
			context.write(new Text(key), new RawData(value));
		}
	}
}