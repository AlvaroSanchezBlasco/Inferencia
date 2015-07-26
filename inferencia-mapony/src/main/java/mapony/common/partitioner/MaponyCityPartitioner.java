package mapony.common.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import mapony.util.constantes.MaponyCiudadesCte;
import mapony.util.writables.RawDataWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         Clase de particionado asociada unicamente al job discriminado por ciudades (Madrid, Londres, Berlin, Roma).
 *         <p>
 *         Dependiendo de la ciudad del registro que recibe, devuelve el numero del reducer al que tiene que mandarse
 *         para ser procesado.
 */
public class MaponyCityPartitioner extends Partitioner<Text, RawDataWritable> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(Text key, RawDataWritable value, int numPartitions) {
		if (value.getCiudad().toString().compareTo(MaponyCiudadesCte.sLondres) == 0) {
			return 1;
		} else if (value.getCiudad().toString().compareTo(MaponyCiudadesCte.sBerlin) == 0) {
			return 2;
		} else if (value.getCiudad().toString().compareTo(MaponyCiudadesCte.sMadrid) == 0) {
			return 3;
		} else if (value.getCiudad().toString().compareTo(MaponyCiudadesCte.sRoma) == 0) {
			return 4;
		} else if (value.getCiudad().toString().compareTo(MaponyCiudadesCte.sParis) == 0) {
			return 5;
		} else if (value.getCiudad().toString().compareTo(MaponyCiudadesCte.sNuevaYork) == 0) {
			return 6;
		} else {
			return 7;
		}
	}
}
