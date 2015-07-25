package mapony.util.constantes;

/**
 * @author Alvaro Sanchez Blasco
 * <p>Clase de constantes<p>Constants
 */
public final class MaponyCte {

	/**
	 * [\\d[^\\w^\\-\\s]]+
	 */
	public static final String PATTERN_SIMBOLOS = "[\\d[^\\w^\\-\\s]]+";

	/**
	 * [^\\w^\\-\\s]+
	 */
	public static final String PATTERN_SIMBOLOS_2 = "[^\\w^\\-\\s]+";

	/**
	 * (\\s{2,})
	 */
	public static final String PATTERN_DOS_ESPACIOS = "(\\s{2,})";
	/**
	 * [\\s{1}][A-Z{2}][\\s{1}]
	 */
	public static final String PATTERN_DOS_MAYUSCULAS = "[\\s{1}][A-Z{2}][\\s{1}]";

	/**
	 * %2B
	 */
	public static final String PATTER_ESPACIO = "%2[A-Z]{1}";

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * g, 1, ~ 5,004km x 5,004km
	 */
	public static final int precisionGeoHashUno = 1;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gc, 2, ~ 1,251km x 625km
	 */
	public static final int precisionGeoHashDos = 2;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcp, 3, ~ 156km x 156km
	 */
	public static final int precisionGeoHashTres = 3;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpu, 4, ~39km x 19.5km
	 */
	public static final int precisionGeoHashCuatro = 4;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpuu, 5, ~ 4.9km x 4.9km
	 */
	public static final int precisionGeoHashCinco = 5;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpuuz, 6, ~ 1.2km x 0.61km
	 */
	public static final int precisionGeoHashSeis = 6;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpuuz9, 7, ~ 152.8m x 152.8m
	 */
	public static final int precisionGeoHashSiete = 7;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpuuz94, 8, ~ 38.2m x 19.1m
	 */
	public static final int precisionGeoHashOcho = 8;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpuuz94k, 9, ~ 4.78m x 4.78m
	 */
	public static final int precisionGeoHashNueve = 9;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpuuz94kk, 10, ~ 1.19m x 0.60m
	 */
	public static final int precisionGeoHashDiez = 10;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpuuz94kkp, 11, ~ 14.9cm x 14.9cm
	 */
	public static final int precisionGeoHashOnce = 11;

	/**
	 * 
	 * Geohash, Level, Dimensions
	 * <p>
	 * gcpuuz94kkp5, 12, ~ 3.7cm x 1.8cm
	 */
	public static final int precisionGeoHashDoce = 12;

	/**
	 * Crea un String informando de la finalización del job que recibe como parámetro
	 * 
	 * @param jobName
	 * @return Mensaje de fin del job
	 */
	public static final String getMsgFinJob(final String jobName) {
		return "\nJob " + jobName + " finalizado\n";
	}

	/**
	 * Crea un String informando que el job que recibe como parámetro va a dar comienzo
	 * 
	 * @param jobName
	 * @return Mensaje de inicio del job
	 */
	public static final String getMsgInicioJob(final String jobName) {
		return "\nComienza la ejecucion del job " + jobName;
	}

	/**
	 * ''
	 */
	public static final String VACIO = "";
	
	/**
	 * |
	 */
	public static final String PIPE = "|";
	
	/**
	 * \\|
	 */
	public static final String ESCAPED_PIPE = "\\|";
	
	/**
	 * \t
	 */
	public static final String TAB = "\t";
	
	/**
	 * -
	 */
	public static final String GUION = "-";
	
	/**
	 * ,
	 */
	public static final String COMA = ",";

	/**
	 * Nombre del Job que agrupa los datos
	 */
	public static final String jobNameGroupNear = "Group Near Job";
	
	/**
	 * Nombre del Job que agrupa los datos
	 */
	public static final String jobNameMayorPrecision = "Precission Job";
	
	/**
	 * Nombre del Job que carga los datos en ES
	 */
	public static final String jobNameMainJob = "Load Job";

	/**
	 * Nombre del job que genera ficheros CVS para su visualizacion en CartoDB
	 */
	public static final String jobNameCsv = "Create CSV Job";
	
	/**
	 * No existen datos en la ruta especificada
	 */
	public static final String MSG_NO_DATOS = "No existen datos en la ruta especificada\nNo data in the specified path";
	
}
