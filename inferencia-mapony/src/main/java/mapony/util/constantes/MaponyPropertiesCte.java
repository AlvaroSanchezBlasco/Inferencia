package mapony.util.constantes;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Clase relacionada con los nombres de las constantes de los ficheros de propiedades obligatorias de cada Job.
 *         <p>
 *         Class that contains constants related to the porperties files that must be used on each Job.
 *         <p>
 */
public class MaponyPropertiesCte {

	/**
	 * Mensaje informativo cuando las propiedades has sido cargadas
	 * <p>
	 * Informative message "Properties successfully loaded".
	 */
	public static final String MSG_PROPIEDADES_CARGADAS = "Properties successfully loaded";

	/**
	 * Constante del fichero de propiedades que indica el tipo que vamos a crear en ES
	 * <p>
	 * Name of the type that we have to create on the ElasticSearch cluster.
	 */
	public static final String tipo = "type_name";

	/**
	 * Constante del fichero de propiedades que indica el nombre del indice que vamos a crear en ES
	 * <p>
	 * Index of the File/path to process in the Job execution.
	 */
	public static final String indice = "index_name";

	/**
	 * Constante del fichero de propiedades que indica el nombre del cluster en el que vamos a hacer la carga de datos
	 * en ES
	 * <p>
	 * Name of the ElasticSearch Cluster
	 */
	public static final String cluster = "clusterName";

	/**
	 * Constante del fichero de propiedades que indica la ip del cluster de ES
	 * <p>
	 * Ip of the ElasticSearch Cluster
	 */
	public static final String ip = "ip";

	/**
	 * Constante del fichero de propiedades que indica el puerto del cluste de ES
	 * <p>
	 * Port of the ElasticSearch Cluster
	 */
	public static final String puerto = "port";

	/**
	 * Constante del fichero de propiedades que indica la ruta a los ficheros a procesar
	 * <p>
	 * Path of the initial data for the Job.
	 */
	public static final String datos_iniciales = "ruta_inicial_fichero";

	/**
	 * Constante del fichero de propiedades que indica la ruta al fichero con la información de los países (obtenido de
	 * geonames)
	 * <p>
	 * Path to the external dataset obtained from geonames
	 */
	public static final String paises = "ruta_paises";

	/**
	 * Constante del fichero de propiedades que indica la ruta en la que dejar los datos de salida el Job de ser
	 * necesario.
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
	 * Indice que indentifica cada archivo a procesar, y además, la usaremos para identificar los directorios de salida
	 * de los jobs
	 */
	public static final String indice_archivo = "indice_archivo";

	/**
	 * En el caso de que haya que concatenar directorio mas tipo de archivos a buscar dentro de un directorio, en esta
	 * variable almacenamos el patro de busqueda de archivos
	 */
	public static final String patron_ficheros = "patron_ficheros";

	/**
	 * Tamanyo del reservoir
	 * <p>
	 * Size of the sample when using the reservoir.
	 */
	public static final String reservoir = "reservoir";

	/**
	 * Constante con la extension de los archivos iniciales
	 * <p>
	 * Extension of the original data
	 */
	public static final String ext_archivos = ".bz2";
}
