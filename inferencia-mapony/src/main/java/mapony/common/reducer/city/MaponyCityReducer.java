package mapony.common.reducer.city;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import mapony.util.constantes.MaponyPropertiesCte;
import mapony.util.patrones.ReservoirSampler;
import mapony.util.writables.RawDataWritable;
import mapony.util.writables.array.RawDataArrayWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         Reducer comun para la ejecucion del algoritmo discriminado por ciudades (Madrid, Londres, Roma, Berlin).
 *         <p>
 *         Dependiendo del nombre de la ciudad asociado al registro(s) procesado(s) emite el resultado a un fichero
 *         asociado con la ciudad a la que pertenece
 *         <p>
 *         Se recorre la coleccion de RawDataWritable que llega como parametro, y se anyade al reservoir de tamanyo
 *         definido en las propiedades del job.
 *         <p>
 *         Al finalizar de cargar el reservoir, se emite una coleccion con las
 *         muestras seleccionadas del mismo en un ArrayWritable con clave el geohash asociado a los registros
 *         procesados.
 */
public class MaponyCityReducer extends Reducer<Text, RawDataWritable, Text, RawDataArrayWritable> {

	private MultipleOutputs<Text, RawDataArrayWritable> mos;
	private int reservoir;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, RawDataArrayWritable>(context);
		reservoir = Integer.parseInt(context.getConfiguration().get(MaponyPropertiesCte.reservoir));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable,
	 * org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<RawDataWritable> values, Context context)
			throws IOException, InterruptedException {

		String ciudad = "indiferente";
		ReservoirSampler<RawDataWritable> r = new ReservoirSampler<RawDataWritable>(reservoir);

		for (RawDataWritable val : values) {
			if (ciudad.equals("indiferente")) {
				ciudad = val.getCiudad().toString();
			}
			r.sample(new RawDataWritable(val));
		}

		Iterable<RawDataWritable> samplesToEmit = r.getSamples();
		ArrayList<RawDataWritable> emit = new ArrayList<RawDataWritable>();
		for (RawDataWritable sampledRDW : samplesToEmit) {
			emit.add(new RawDataWritable(sampledRDW));
		}

		mos.write(new Text(key), new RawDataArrayWritable(Text.class, emit.toArray(new RawDataWritable[emit.size()])),
				ciudad);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
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