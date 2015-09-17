package mapony.inferencia.util;

import org.apache.hadoop.io.Text;

import ch.hsr.geohash.GeoHash;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.exception.InferenciaException;
import mapony.inferencia.util.validation.ComplexValidation;
import mapony.inferencia.util.validation.SimpleValidation;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Clase de utilidades.
 *         <p>
 *         Utilities
 */
public class Utilities {

	void monad(Integer... args) {
	}

	void dyad(String name, Integer... args) {
	}

	void triad(String name, int count, Integer... args) {
	}

	/**
	 * Metodo que compara el Text que llega como parametro con null y '' para verificar si tiene valor o no, haciendo uso del metodo equals
	 * implementado en la clase Text.
	 * 
	 * @param cadena
	 * @return true en caso de que no sea nulo ni vacio.
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
	 * The first step removes all characters that are not a letter or a space and replaces them with a space. The second step removes multiple spaces
	 * by only one space.
	 * 
	 * @param cadena
	 * @return la cadena limpia de caracteres extranyos
	 */
	public static final String cleanString(String cadena) {
		if (ComplexValidation.isTrimExpectedNotNullAndNotEmpty(cadena)) {
			cadena = cadena.replaceAll(InferenciaCte.PATTER_ESPACIO, " ").replaceAll(InferenciaCte.PATTERN_SIMBOLOS_2, " ");
			cadena = cleanPreposiciones(cadena);
			cadena = cadena.replaceAll(InferenciaCte.PATTERN_DOS_MAYUSCULAS, "").replaceAll(InferenciaCte.PATTERN_DOS_ESPACIOS, " ");
			return cadena;
		} else
			return InferenciaCte.GUION;
	}

	/**
	 * Eliminamos parabras que se suelen repetir en los campos de descripciones, para que no efecten en las estadisticas de nuestro resultado.
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
		str = str.replaceAll("[\\s]{1,}segÃºn{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}segun{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}sin{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}so{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}sobre{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}tras{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}versus{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}vÃ­a{1,}[\\s]{1,}", " ");
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
		str = str.replaceAll("[\\s]{1,}SEGÃšN{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SEGUN{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SIN{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SO{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}SOBRE{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}TRAS{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}VERSUS{1,}[\\s]{1,}", " ");
		str = str.replaceAll("[\\s]{1,}VÃ�A{1,}[\\s]{1,}", " ");
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
	 * Recibidos los parametros de posicion, y precision del geohash, devuelve un String con el geohash calculado.
	 * 
	 * @param position
	 * @param precision
	 * @return
	 * @throws InferenciaException
	 */
	public static final String getGeoHashAsStringByPrecission(final Position position, int precision) throws InferenciaException {
		try {
			return GeoHash.geoHashStringWithCharacterPrecision(position.getLatitude(), position.getLongitude(), precision);
		} catch (Exception e) {
			throw new InferenciaException(e, e.getMessage());
		}
	}

	/**
	 * @param fecha
	 *            (2008-02-09 16:27:11.0)
	 * @return 2008-02-09
	 */
	public static final String getElasticSearchDateFieldFromString(String fecha) {
		if (SimpleValidation.isExpectedNotNull(fecha)) {
			if (fecha.length() > 10) {
				// 2008-02-09 16:27:11.0
				return fecha.substring(0, 10);
			} else
				return InferenciaCte.EMPTY_STRING;
		} else
			return InferenciaCte.EMPTY_STRING;
	}

}
