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

	@Override
	public int getPartition(Text key, RawData value, int numPartitions) {
		
		final String ciudad = value.getCiudad().toString(); 

		if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sLondres)) {
			return 1;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sBerlin)) {
			return 1;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sMadrid)) {
			return 1;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sRoma)) {
			return 1;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sParis)) {
			return 1;
		} else if (SimpleValidation.isTrimExpectedEqualsTrimActual(ciudad, CitiesCte.sNuevaYork)) {
			return 1;
		} else {
			return 2;
		}
	}
}
