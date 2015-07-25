package mapony.util.constantes;

/**
 * @author Alvaro Sanchez Blasco
 *         TODO anyadir aqui todas las constantes relacionadas con el fichero de propiedades.
 *         TODO Anyadir todas las constantes nuevas del fichero de propiedades. Confirmar que con el {indice} se obtiene
 *         el resultado deseado.
 */
public class MaponyPropertiesCte {

	/**
	 * Mensaje informativo de propiedades cargadas
	 */
	public static final String MSG_PROPIEDADES_CARGADAS = "Propiedades cargadas";

	/** Conexión con ElasticSearch */
	/**
	 * Constante del fichero de propiedades que indica el tipo que vamos a crear en ES
	 */
	public static final String tipo = "type_name";
	
	/**
	 * Constante del fichero de propiedades que indica el nombre del indice que vamos a crear en ES
	 */
	public static final String indice = "index_name";

	/**
	 * Constante del fichero de propiedades que indica el nombre del cluster en el que vamos a hacer la carga de datos
	 * en ES
	 */
	public static final String cluster = "clusterName";
	
	/**
	 * Constante del fichero de propiedades que indica la ip del cluster de ES
	 */
	public static final String ip = "ip";

	/**
	 * Constante del fichero de propiedades que indica el puerto del cluste de ES
	 */
	public static final String puerto = "port";
	
	/** Conexión con ElasticSearch */

	/**
	 * Constante del fichero de propiedades que indica la ruta a los ficheros a procesar
	 */
	public static final String datos_iniciales = "ruta_inicial_fichero";

	/**
	 * Constante del fichero de propiedades que indica la ruta al fichero con la información de los países (obtenido de
	 * geonames)
	 */
	public static final String paises = "ruta_paises";
	
	/**
	 * Constante del fichero de propiedades que indica la ruta en la que dejar los datos de salida el Job GroupNear.
	 * Concatenar
	 * el indice en Java.
	 */
	public static final String salida_datos_job = "ruta_salida_job";

	/**
	 * Constante del fichero de propiedades que indica el numero de reducers para nuestro job
	 */
	public static final String reducers = "numero_reducer";
	
	/**
	 * Precision para el calculo del geohash
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
	 * */
	public static final String reservoir = "reservoir";
	
	/**
	 * Constante con la extension de los archivos iniciales
	 */
	public static final String ext_archivos = ".bz2";
}
