package mapony.city.a_groupNear.mapper;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.util.beans.CustomGeoHashBean;
import mapony.util.clases.GeoHashCiudad;
import mapony.util.clases.MaponyUtil;
import mapony.util.constantes.MaponyCte;
import mapony.util.constantes.MaponyPropertiesCte;
import mapony.util.writables.RawDataWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Mapper para el Job GroupNear Discriminado.
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
 *         Además, si no tiene informados los campos de longitud y latitud, también descartamos el registro.
 *         <p>
 *         Por ultimo, se descartan tambien aquellos registros que no tengan la fecha de captura.
 *         <p>
 *         Recuperamos de memoria un HashMap que contiene unicamente los datos de Madrid, Berlin, Roma y Londres. Se
 *         compara el geohash de cada registro procesado con el de estas ciudades y solo se emite resultado si
 *         coinciden.
 */
public class MaponyGroupNearByCityMap extends Mapper<LongWritable, Text, Text, RawDataWritable> {

	private static final Logger logger = LoggerFactory.getLogger(MaponyGroupNearByCityMap.class);
	private HashMap<String, CustomGeoHashBean> ciudades;
	private int precisionGeoHash;

	protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

		String[] dato = line.toString().split(MaponyCte.TAB);

		if ("1".toString().compareTo(dato[22]) != 0
				&& (MaponyCte.VACIO.compareTo(dato[10]) != 0 && MaponyCte.VACIO.compareTo(dato[11]) != 0)) {
			try {
				if (MaponyUtil.stringTieneValor(dato[3])) {

					final Text longitud = new Text(dato[10]);
					final Text latitud = new Text(dato[11]);

					final Text geoHash = MaponyUtil.getGeoHashPorPrecision(longitud, latitud, precisionGeoHash);

					Text ciudad = new Text();

					// Solo emitimos valores que correspondan con las ciudades que queremos procesar
					if (ciudades.containsKey(geoHash.toString())) {
						final CustomGeoHashBean temp = ciudades.get(geoHash.toString());
						ciudad = new Text(temp.getName());
						RawDataWritable rdBean = new RawDataWritable(new Text(dato[0]), new Text(dato[3]),
								new Text(dato[5]), new Text(dato[6]), new Text(dato[7]), new Text(dato[8]),
								new Text(dato[9]), new Text(dato[10]), new Text(dato[11]), new Text(dato[14]),
								new Text(geoHash.toString()), new Text(ciudad.toString()));

						context.write(new Text(rdBean.getGeoHash().toString()), new RawDataWritable(rdBean));
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
	}

	public void setup(Context context) {
		precisionGeoHash = Integer.parseInt(context.getConfiguration().get(MaponyPropertiesCte.precision));
		try {
			if (null == ciudades) {
				ciudades = GeoHashCiudad.cargaGeoHashCiudadesSeleccionadas(precisionGeoHash);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
}
