// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.partitioner;

import mapony.inferencia.util.cte.CitiesCte;
import mapony.inferencia.util.validation.SimpleValidation;
import mapony.inferencia.writables.RawData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Alvaro Sanchez Blasco
 *         Clase de particionado asociada unicamente al job discriminado por ciudades.
 *         <p>
 *         Dependiendo de la ciudad del registro que recibe, devuelve el numero del reducer al que tiene que mandarse
 *         para ser procesado.
 */
public class CartoDbPartitioner extends Partitioner<Text, RawData> {

	// Bug fix: the original code returned partition 1 for all known cities and
	// partition 2 for others. With numReducers=2, partition 2 is out of bounds
	// (valid range: 0..numReducers-1). The fix assigns each of the six cities its
	// own partition (0-5) for balanced parallel output, and sends unrecognised
	// records to partition 0. numReducers in the properties file must be >= 6.
	@Override
	public int getPartition(Text key, RawData value, int numPartitions) {

		final String ciudad = value.getCiudad().toString();

		if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sLondres)) {
			return 0;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sBerlin)) {
			return 1;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sMadrid)) {
			return 2;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sRoma)) {
			return 3;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sParis)) {
			return 4;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sNuevaYork)) {
			return 5;
		} else {
			// Records with no recognised city share partition 0 (London's reducer).
			return 0;
		}
	}
}
