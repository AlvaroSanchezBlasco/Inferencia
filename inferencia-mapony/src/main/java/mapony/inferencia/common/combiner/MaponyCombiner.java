package mapony.inferencia.common.combiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapony.util.writables.RawDataWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Clase de combiner comun a todos los jobs que la requieran.
 *         <p>
 *         Combina los datos antes de mandarlos a los reducer.
 *         <p>
 *         Common combiner class.
 *         <p>
 *         Combines the records before send them to the Reducer.
 */
public class MaponyCombiner extends Reducer<Text, RawDataWritable, Text, RawDataWritable> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable,
	 * org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<RawDataWritable> values, Context context)
			throws IOException, InterruptedException {
		for (RawDataWritable value : values) {
			context.write(new Text(key), new RawDataWritable(value));
		}
	}
}