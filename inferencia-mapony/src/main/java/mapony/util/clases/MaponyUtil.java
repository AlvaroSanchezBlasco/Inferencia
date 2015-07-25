package mapony.util.clases;

import org.apache.hadoop.io.Text;

import ch.hsr.geohash.GeoHash;
import mapony.util.constantes.MaponyCte;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Clase de utilidades.
 *         <p>
 *         Utilities
 */
public class MaponyUtil {

	/**
	 * Metodo que compara el String que llega como parametro con null y '' para verificar si tiene valor o no.
	 * <p>
	 * Compares the parameter String whith null & ''.
	 * 
	 * @param cadena
	 * @return true en caso de que no sea nulo ni vacío.
	 *         <p>
	 *         true if the String is not empty
	 */ 
	public static boolean stringTieneValor(String cadena) {
		boolean tieneValor = false;

		if (cadena != null && !(MaponyCte.VACIO.compareTo(cadena) == 0) && !(MaponyCte.GUION.compareTo(cadena) == 0)) {
			tieneValor = true;
		}
		return tieneValor;
	}

	/**
	 * Metodo que compara el valor String del Text que llega como parametro con null y '' para verificar si tiene valor
	 * o no.
	 * 
	 * @param cadena
	 * @return true en caso de que no sea nulo ni vacío.
	 */
	public static boolean textTieneValor(Text cadena) {
		boolean tieneValor = false;
		if (cadena != null) {
			Text vacio = new Text();
			if (!vacio.equals(cadena)) {
				tieneValor = true;
			}
		}
		return tieneValor;
	}

	/**
	 * The first step removes all characters that are not a letter or a space and replaces them with a space. The second
	 * step removes multiple spaces by only one space.
	 * 
	 * @param cadena
	 * @return la cadena limpia de caracteres extranyos
	 */
	public static final String cleanString(String cadena) {
		if (stringTieneValor(cadena)) {
			cadena = cadena.replaceAll(MaponyCte.PATTER_ESPACIO, " ").replaceAll(MaponyCte.PATTERN_SIMBOLOS_2, " ");
			cadena = cleanPreposiciones(cadena);
			cadena = cadena.replaceAll(MaponyCte.PATTERN_DOS_MAYUSCULAS, "").replaceAll(MaponyCte.PATTERN_DOS_ESPACIOS,
					" ");
			return cadena;
		} else
			return MaponyCte.GUION;
	}

	/**
	 * Eliminamos parabras que se suelen repetir en los campos de descripciones, para que no efecten en las estadisticas
	 * de nuestro resultado.
	 * 
	 * @param str
	 * @return String sin las cadenas definidas dentro del metodo.
	 */
	private static String cleanPreposiciones(String str) {
		str = str.replaceAll("[\\s]{1,}ante{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}bajo{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}cabe{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}con{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}contra{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}de{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}desde{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}durante{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}en{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}entre{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}hacia{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}hasta{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}mediante{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}para{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}por{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}según{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}segun{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}sin{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}so{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}sobre{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}tras{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}versus{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}vía{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}el{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}la{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}lo{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}los{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}las{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}les{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}juan{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}jose{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}ana{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}ANTE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}BAJO{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}CABE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}CON{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}CONTRA{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}DE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}DESDE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}DURANTE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}EN{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}ENTRE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}HACIA{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}HASTA{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}MEDIANTE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}PARA{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}POR{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SEGÚN{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SEGUN{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SIN{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SO{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SOBRE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}TRAS{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}VERSUS{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}VÍA{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}EL{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}LA{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}LO{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}LOS{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}LAS{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}LES{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}JUAN{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}JOSE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}ANA{1,}[\\s]{1,}", " ");
		return str;
	}

	/**
	 * Recibidos los parametros de longitud, latitud, y precision del geohash, devuelve el geohash calculado.
	 * 
	 * @param longitude
	 * @param latitude
	 * @param precision
	 * @return El geohash calculado
	 * @throws Exception
	 */
	public static final Text getGeoHashPorPrecision(Text longitude, Text latitude, int precision) throws Exception {
		try {
			double dLatitude = new Double(latitude.toString());
			double dLongitude = new Double(longitude.toString());
			return new Text(GeoHash.geoHashStringWithCharacterPrecision(dLatitude, dLongitude, precision));
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * Recibidos los parametros de longitud, latitud, y precision del geohash, devuelve el geohash calculado.
	 * 
	 * @param longitude
	 * @param latitude
	 * @param precision
	 * @return El geohash calculado
	 * @throws Exception
	 */
	public static final String getStringGeoHashPorPrecision(String longitude, String latitude, int precision)
			throws Exception {
		try {
			double dLatitude = new Double(latitude);
			double dLongitude = new Double(longitude);
			return GeoHash.geoHashStringWithCharacterPrecision(dLatitude, dLongitude, precision);
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * Recibidos los parametros de posicion[] ([0]latitud,[1]longitud), y precision del geohash, devuelve el geohash
	 * calculado.
	 * 
	 * @param posicion
	 * @param precision
	 * @return El geohash calculado
	 * @throws Exception
	 */
	public static final Text getGeoHashPorPrecision(String posicion, int precision) throws Exception {
		try {
			String[] pos = posicion.split(",");
			double dLatitude = new Double(pos[0]);
			double dLongitude = new Double(pos[1]);

			return new Text(GeoHash.geoHashStringWithCharacterPrecision(dLatitude, dLongitude, precision));
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * @param fecha
	 *            (2008-02-09 16:27:11.0)
	 * @return 2008-02-09
	 */
	public static final String getFechaFromString(String fecha) {
		if (fecha != null) {
			if (fecha.length() > 10) {
				// 2008-02-09 16:27:11.0
				return fecha.substring(0, 10);
			} else
				return MaponyCte.VACIO;
		} else
			return MaponyCte.VACIO;
	}

}
