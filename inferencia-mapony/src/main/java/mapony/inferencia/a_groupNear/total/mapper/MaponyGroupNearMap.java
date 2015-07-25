package mapony.inferencia.a_groupNear.total.mapper;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.util.beans.GeoHashBean;
import mapony.util.clases.GeoHashCiudad;
import mapony.util.clases.MaponyUtil;
import mapony.util.constantes.MaponyCte;
import mapony.util.constantes.MaponyPropertiesCte;
import mapony.util.writables.RawDataWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Mapper para el Job GroupNear.
 *         <p>
 *         Recibe una linea de texto, y la parsea a un CustomWritable que tiene los campos:
 *         <p>
 *         identifier
 *         <p>
 *         dateTaken
 *         <p>
 *         captureDevice
 *         <p>
 *         title
 *         <p>
 *         description
 *         <p>
 *         userTags
 *         <p>
 *         machineTags
 *         <p>
 *         longitude
 *         <p>
 *         latitude
 *         <p>
 *         downloadUrl
 *         <p>
 *         geoHash
 *         <p>
 *         continente
 *         <p>
 *         pais
 *         <p>
 *         ciudad
 *         <p>
 *         Emite como clave el GeoHash calculado con la precision que se ha definido para el job, y el RawDataWritale
 *         generado
 *         <p>
 *         De los datos de entrada, Comenzamos por limpiar las referencias de videos, por lo que el campo [22] del
 *         String[] ha de ser
 *         '0' para que lo procesemos.
 *         <p>
 *         Ademas, si no tiene informados los campos de longitud y latitud, tambien descartamos el registro.
 *         <p>
 *         Por ultimo, se descartan tambien aquellos registros que no tengan la fecha de captura.
 */
public class MaponyGroupNearMap extends Mapper<LongWritable, Text, Text, RawDataWritable> {

	/**
	 * Logger para la clase MaponyGroupNearMap
	 */
	private static final Logger logger = LoggerFactory.getLogger(MaponyGroupNearMap.class);
	/**
	 * HashMap con las ciudades obtenidas del fichero de geonames
	 */
	private HashMap<String, GeoHashBean> ciudades;
	/**
	 * Precision definida en el Job para el calculo del geohash que servira de clave
	 */
	private int precisionGeoHash;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

		String[] dato = line.toString().split(MaponyCte.TAB);

		// Comenzamos por limpiar las referencias de videos, por lo que el campo [22] del String[] ha de ser
		// '0' para que lo procesemos.
		// Además, si no tiene informados los campos de longitud y latitud, también descartamos el registro.
		if ("1".toString().compareTo(dato[22]) != 0
				&& (MaponyCte.VACIO.compareTo(dato[10]) != 0 && MaponyCte.VACIO.compareTo(dato[11]) != 0)) {
			try {

				// Al realizar la carga en ES, como queremos informar de la fecha de captura, si esta no viene, da un
				// error, y no se cargan datos. Vamos a discriminar también los datos que no tienen el campo fecha de
				// captura (dateTaken) informado.
				if (MaponyUtil.stringTieneValor(dato[3])) {

					final Text longitud = new Text(dato[10]);
					final Text latitud = new Text(dato[11]);

					final Text geoHash = MaponyUtil.getGeoHashPorPrecision(longitud, latitud, precisionGeoHash);

					// Buscamos correspondencia entre el geohash calculado, y las ciudades con las que estamos
					// trabajando, para completar la informacion del writable
					Text ciudad = new Text();
					Text pais = new Text();
					Text continente = new Text();
					if (ciudades.containsKey(geoHash.toString())) {
						final GeoHashBean temp = ciudades.get(geoHash.toString());
						ciudad = new Text(temp.getName());
						pais = new Text(temp.getPais());
						continente = new Text(temp.getContinente());
					}
					// Creamos el Writable
					RawDataWritable rdBean = new RawDataWritable(new Text(dato[0]), new Text(dato[3]),
							new Text(dato[5]), new Text(dato[6]), new Text(dato[7]), new Text(dato[8]),
							new Text(dato[9]), new Text(dato[10]), new Text(dato[11]), new Text(dato[14]),
							new Text(geoHash.toString()), new Text(continente.toString()), new Text(pais.toString()),
							new Text(ciudad.toString()));

					// Emitimos los datos
					context.write(new Text(rdBean.getGeoHash().toString()), new RawDataWritable(rdBean));
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void setup(Context context) {
		precisionGeoHash = Integer.parseInt(context.getConfiguration().get(MaponyPropertiesCte.precision));
		try {
			if (null == ciudades) {
				ciudades = GeoHashCiudad.getDatos();
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
}
