package mapony.inferencia.util.cte;

/**
 * @author Alvaro Sanchez Blasco
 *         <ul>
 *         <li>[ES] Clase relacionada con los nombres de las constantes de los ficheros de propiedades obligatorias de cada Job.
 *         <li>[EN] Class that contains constants related to the porperties files that must be used on each Job.
 *         </ul>
 */
public class PropertiesCte {

	/**
	 * <ul>
	 * <li>[ES] Uso del Jar. Necesita un fichero de propiedades para poder ejecutarse.
	 * <li>[EN] Job usage. Needs a properties file to execute.
	 * </ul>
	 */
	public static final String usage = "Usage: <config.properties file>\n";

	/**
	 * Mensaje informativo cuando las propiedades has sido cargadas
	 * <p>
	 * Informative message "Properties successfully loaded".
	 */
	public static final String PROPERTIES_LOADED = "Properties successfully loaded";

	/**
	 * Constante del fichero de propiedades que indica la ruta a los ficheros a procesar
	 * <p>
	 * Path of the initial data for the Job.
	 */
	public static final String datos_iniciales = "ruta_inicial_fichero";

	/**
	 * Constante del fichero de propiedades que indica la ruta al fichero con la información de los países (obtenido de geonames)
	 * <p>
	 * Path to the external dataset obtained from geonames
	 */
	public static final String paises = "ruta_paises";

	/**
	 * Constante del fichero de propiedades que indica la ruta en la que dejar los datos de salida el Job de ser necesario.
	 * <p>
	 * Path where the Job will emit the data (if needed).
	 */
	public static final String salida_datos_job = "ruta_salida_job";

	/**
	 * Constante del fichero de propiedades que indica el numero de reducers para nuestro job
	 * <p>
	 * #reducers to use in the Job.
	 */
	public static final String reducers = "numero_reducer";

	/**
	 * Precision para el calculo del geohash
	 * <p>
	 * GeoHash precission
	 */
	public static final String precision = "precision";

	/**
	 * Indice que indentifica cada archivo a procesar, y además, la usaremos para identificar los directorios de salida de los jobs
	 */
	public static final String indice_archivo = "indice_archivo";

	/**
	 * En el caso de que haya que concatenar directorio mas tipo de archivos a buscar dentro de un directorio, en esta variable almacenamos el patro
	 * de busqueda de archivos
	 */
	public static final String patron_ficheros = "patron_ficheros";

	/**
	 * Tamanyo del reservoir
	 * <p>
	 * Size of the sample when using the reservoir.
	 */
	public static final String reservoirSize = "reservoir";

	/**
	 * Constante con la extension de los archivos iniciales
	 * <p>
	 * Extension of the original data
	 */
	public static final String ext_archivos = ".bz2";
}
