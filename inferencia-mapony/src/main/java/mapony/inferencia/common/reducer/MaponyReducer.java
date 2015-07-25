package mapony.inferencia.common.reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapony.util.constantes.MaponyPropertiesCte;
import mapony.util.patrones.ReservoirSampler;
import mapony.util.writables.RawDataWritable;
import mapony.util.writables.array.RawDataArrayWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         Reducer comun para la ejecucion del agrupacion.
 *         <p>
 *         Se recorre la coleccion de RawDataWritable que llega como parametro, y se anyade al reservoir de tamanyo
 *         definido en las propiedades del job.
 *         <p>
 *         Al finalizar de cargar el reservoir, se emite una colección con las
 *         muestras seleccionadas del mismo en un ArrayWritable con clave el geohash asociado a los registros
 *         procesados.
 */
public class MaponyReducer extends Reducer<Text, RawDataWritable, Text, RawDataArrayWritable> {

	private int reservoir;

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<RawDataWritable> values, Context context)
			throws IOException, InterruptedException {
		//Creamos un reservoir de tamanyo definido en las propiedades del job.
		ReservoirSampler<RawDataWritable> r = new ReservoirSampler<RawDataWritable>(reservoir);
		for (RawDataWritable rawDataWritable : values) {
			r.sample(new RawDataWritable(rawDataWritable));
		}
		
		//Recuperamos las muestras del reservoir
		Iterable<RawDataWritable> samplesToEmit = r.getSamples();
		//Creamos la lista de valores que vamosa emitir 
		ArrayList<RawDataWritable> emit = new ArrayList<RawDataWritable>();
		//Iteramos sobre las muestras, cargando los valores en la lista a emitir
		for (RawDataWritable sampledRDW : samplesToEmit) {
			emit.add(new RawDataWritable(sampledRDW));
		}
		
		context.write(new Text(key),
				new RawDataArrayWritable(Text.class, emit.toArray(new RawDataWritable[emit.size()])));
		//Limpiamos la lista para liberar memoria.
//		emit.clear();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void setup(Context context) {
		reservoir = Integer.parseInt(context.getConfiguration().get(MaponyPropertiesCte.reservoir));
		System.err.println(reservoir);
	}
}