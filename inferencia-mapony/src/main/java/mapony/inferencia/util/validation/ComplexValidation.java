package mapony.inferencia.util.validation;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.inferencia.geoHash.bean.GeoHashBean;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.exception.InferenciaException;
import mapony.inferencia.writables.RawData;

public class ComplexValidation {
	private static Logger logger = LoggerFactory.getLogger(ComplexValidation.class);

	private static final String INVALID_NUMBER_OF_ARGUMENTS = "Invalid number of arguments in param String[]. Expected 22, recieved %s";
	private static final String RECORD_IS_VIDEO = "The record matches video data.";
	private static final String LONGITUDE_IS_NULL_OR_EMPTY = "Param longitude is null or empty.";
	private static final String LATITUDE_IS_NULL_OR_EMPTY = "Param latitude is null or empty.";
	private static final String DATE_TAKEN_IS_NULL_OR_EMPTY = "Param dateTaken is null or empty.";
	private static final String DATE_TAKEN_HAS_NO_VALID_VALUE = "Param dateTaken has no valid value.";
	private static final String GEOHASHBEAN_IS_NULL = "The param GeoHashBean is null.";
	private static final String GEOHASHBEAN_CITY_IS_NULL_OR_EMPTY = "The city of the GeoHashBean is null or empty.";
	private static final String GEOHASHBEAN_GEOHASH_IS_NULL_OR_EMPTY = "The geoHash of the GeoHashBean is null or empty.";

	/**
	 * De los datos de entrada, Comenzamos por limpiar las referencias de videos, por lo que el campo [22] del String[] ha de ser '0' para que lo
	 * procesemos.
	 * <p>
	 * Ademas, si no tiene informados los campos de longitud (dato[10]) y latitud (dato[11]), tambien descartamos el registro.
	 * <p>
	 * Por ultimo, se descartan tambien aquellos registros que no tengan la fecha de captura (dato[3]).
	 * 
	 * @param dato
	 * @return true if the record is useful
	 * @throws InferenciaException
	 */
	public static boolean isRecordUseful(String[] dato) throws InferenciaException {

		if (dato.length < 22) {
			throw new InferenciaException(new Exception(INVALID_NUMBER_OF_ARGUMENTS), INVALID_NUMBER_OF_ARGUMENTS);
		} else {
			// Formato del registro
			if (SimpleValidation.isNotTrimExpectedEqualsTrimActual(InferenciaCte.PHOTO_FORMAT, dato[22])) {
				logger.debug(RECORD_IS_VIDEO);
				return false;
			}

			// Longitud
			if (isTrimExpectedNullOrEmpty(dato[10])) {
				logger.debug(LONGITUDE_IS_NULL_OR_EMPTY);
				return false;
//				throw new InferenciaException(new Exception(LONGITUDE_IS_NULL_OR_EMPTY), LONGITUDE_IS_NULL_OR_EMPTY);
			}

			// Latitud
			if (isTrimExpectedNullOrEmpty(dato[11])) {
				logger.debug(LATITUDE_IS_NULL_OR_EMPTY);
				return false;
//				throw new InferenciaException(new Exception(LATITUDE_IS_NULL_OR_EMPTY), LATITUDE_IS_NULL_OR_EMPTY);
			}

			// Campo fecha de captura
			if (isTrimExpectedNullOrEmpty(dato[3])) {
				logger.debug(DATE_TAKEN_IS_NULL_OR_EMPTY);
				return false;
//				throw new InferenciaException(new Exception(DATE_TAKEN_IS_NULL_OR_EMPTY), DATE_TAKEN_IS_NULL_OR_EMPTY);
			}

			if (SimpleValidation.isTrimExpectedEqualsTrimActual(dato[3], InferenciaCte.GUION)) {
				logger.debug(DATE_TAKEN_HAS_NO_VALID_VALUE);
				return false;
//				throw new InferenciaException(new Exception(DATE_TAKEN_HAS_NO_VALID_VALUE), DATE_TAKEN_HAS_NO_VALID_VALUE);
			}

			return true;
		}
	}

	public static boolean isCustomWritableUseful(RawData dato) throws InferenciaException {
		// Longitud
		if (isTextNotNullOrNotEmpty(dato.getLongitude())) {
			throw new InferenciaException(new Exception(LONGITUDE_IS_NULL_OR_EMPTY), LONGITUDE_IS_NULL_OR_EMPTY);
		}

		// Latitud
		if (isTextNotNullOrNotEmpty(dato.getLatitude())) {
			throw new InferenciaException(new Exception(LATITUDE_IS_NULL_OR_EMPTY), LATITUDE_IS_NULL_OR_EMPTY);
		}

		// Campo fecha de captura
		if (isTextNotNullOrNotEmpty(dato.getDateTaken())) {
			throw new InferenciaException(new Exception(DATE_TAKEN_IS_NULL_OR_EMPTY), DATE_TAKEN_IS_NULL_OR_EMPTY);
		}

		return true;
	}
	
	public static boolean isGeoHashUseful(GeoHashBean beanFromGeonames) throws InferenciaException {
		if (SimpleValidation.isObjectNull(beanFromGeonames)) {
			throw new InferenciaException(new Exception(GEOHASHBEAN_IS_NULL), GEOHASHBEAN_IS_NULL);
		}
		if (isTrimExpectedNullOrEmpty(beanFromGeonames.getCity())) {
			throw new InferenciaException(new Exception(GEOHASHBEAN_CITY_IS_NULL_OR_EMPTY), GEOHASHBEAN_CITY_IS_NULL_OR_EMPTY);
		}
		if (isTrimExpectedNullOrEmpty(beanFromGeonames.getGeoHash())) {
			throw new InferenciaException(new Exception(GEOHASHBEAN_GEOHASH_IS_NULL_OR_EMPTY), GEOHASHBEAN_GEOHASH_IS_NULL_OR_EMPTY);
		}
		return true;
	}

	/**
	 * Compares the param String with null and an empty String.
	 * 
	 * @param expected
	 * @return Returns true if expected has value.
	 */
	public static boolean isTrimExpectedNotNullAndNotEmpty(String expected) {
		if (SimpleValidation.isExpectedNotNull(expected) && !SimpleValidation.isTrimExpectedEqualsEmpty(expected)) {
			return true;
		}
		return false;
	}

	/**
	 * Compares the param String with null and an empty String.
	 * 
	 * @param expected
	 * @return Returns true if expected is null or empty.
	 */
	public static boolean isTrimExpectedNullOrEmpty(String expected) {
		if (SimpleValidation.isStringExpectedNull(expected) || SimpleValidation.isTrimExpectedEqualsEmpty(expected)) {
			return true;
		}
		return false;
	}

	public static boolean isTextNullOrEmpty(Text expected) {
		if(SimpleValidation.isObjectNull(expected) || SimpleValidation.isTextValueEmpty(expected)){
			return true;
		}
		return false;
	}
	
	public static boolean isTextNotNullOrNotEmpty(Text expected) {
		if(SimpleValidation.isObjectNotNull(expected) && !SimpleValidation.isTextValueEmpty(expected)){
			return true;
		}
		return false;
	}
	
	
}
