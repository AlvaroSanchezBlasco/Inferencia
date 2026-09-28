package mapony.inferencia.util.driver;

import mapony.inferencia.jobs.cartodb.CartoDB;
import mapony.inferencia.jobs.groupNear.GroupNear;
import mapony.inferencia.jobs.groupNear.bycity.GroupNearByCity;
import mapony.inferencia.jobs.load.Load;
import mapony.inferencia.jobs.precission.Precission;
import mapony.inferencia.jobs.precission.bycity.PrecissionByCity;

/**
 * @author Alvaro Sanchez Blasco
 *         <ul>
 *         <li>[ES] Clase para controlar las ejecuciones de los Job.
 *         <li>[EN] Class to control the Job's executions.
 *         </ul>
 */
public class Driver extends ProgramDriver {

	/**
	 * Group By Near Places Job.
	 * <ul>
	 * <li>[ES] Primer Job a ejecutar. Agrupa los datos por ciudades.
	 * <li>[EN] First Job to run. Arrange data by populated cities.
	 * </ul>
	 */
	private final String DESC_GROUP_NEAR = "\tGroup By Near Places Job. \n\t\tPrimer Job a ejecutar. Agrupa los datos por ciudades.\n\t\tFirst Job to run. Arrange data by populated cities.";

	/**
	 * Group By Near Places Job.
	 * <ul>
	 * <li>[ES] Primer Job a ejecutar. Agrupa los datos de las ciudades: Madrid, Londres, Berlin, Roma, Paris, Nueva York.
	 * <li>[EN] First Job to run. Arrange data by the cities: Madrid, London, Berlin, Rome, Paris, New York.
	 * </ul>
	 */
	private final String DESC_GROUP_NEAR_CITIES = "\tGroup By Near Places Selected Cities Job. \n\t\tPrimer Job a ejecutar. Agrupa los datos de las ciudades: Madrid, Londres, Berlin, Roma, Paris, Nueva York.\n\t\tFirst Job to run. Arrange data by the cities: Madrid, London, Berlin, Rome, Paris, New York.";

	/**
	 * Precission Job.
	 * <ul>
	 * <li>[ES] Segundo Job a ejecutar. Agrupa con mayor precision los datos.
	 * <li>[EN] Second Job to run. Gives more accuracy to arrange data.
	 * </ul>
	 */
	private final String DESC_PRECISSION = "\tPrecission Job. \n\t\tSegundo Job a ejecutar. Agrupa con mayor precision los datos.\n\t\tSecond Job to run. Gives more accuracy to arrange data.";

	/**
	 * Precission Job.
	 * <ul>
	 * <li>[ES] Segundo Job a ejecutar. Agrupa con mayor precision los de las ciudades: Madrid, Londres, Berlin, Roma, Paris, Nueva York.
	 * <li>[EN] Second Job to run. Gives more accuracy to arrange data of the cities: Madrid, London, Berlin, Rome, Paris, New York.
	 * </ul>
	 */
	private final String DESC_PRECISSION_CITIES = "\tPrecission Job. \n\t\tSegundo Job a ejecutar. Agrupa con mayor precision los de las ciudades: Madrid, Londres, Berlin, Roma, Paris, Nueva York.\n\t\tSecond Job to run. Gives more accuracy to arrange data of the cities: Madrid, London, Berlin, Rome, Paris, New York.";

	/**
	 * Load Job.
	 * <ul>
	 * [ES]
	 * <li>Descripcion para el Job load
	 * <li>Tercer Job a ejecutar. Carga los datos en un cluster de ElasticSearch.
	 * </ul>
	 * <ul>
	 * [EN]
	 * <li>Load Job's description
	 * <li>Third Job to run. Loads data into an ElasticSearch cluster.
	 * </ul>
	 *
	 */
	private final String DESC_LOAD = "\tLoad Job. \n\t\tTercer Job a ejecutar. Carga los datos en un cluster de ElasticSearch.\n\t\tThird Job to run. Loads data into a ElasticSearch cluster.";

	/**
	 * CartoDB CSV Job.
	 * <ul>
	 * [ES]
	 * <li>Genera un fichero CSV con los datos de las ciudades procesadas, listo para cargar en CartoDB.
	 * <li>Ejecutar opcionalmente tras precission o precissionCities.
	 * </ul>
	 * <ul>
	 * [EN]
	 * <li>Generates a CSV file with the processed city data, ready for CartoDB ingestion.
	 * <li>Run optionally after precission or precissionCities.
	 * </ul>
	 * Bug fix: CartoDB was a fully implemented job but was never registered here,
	 * making it unreachable via the Driver command-line interface.
	 */
	private final String DESC_CARTODB = "\tCartoDB CSV Job. \n\t\tGenera un CSV listo para carga en CartoDB.\n\t\tGenerates a CSV file ready for CartoDB ingestion.";

	/**
	 * @throws Throwable
	 */
	public Driver() throws Throwable {
		super();
		addClass("groupNear", GroupNear.class, DESC_GROUP_NEAR);
		addClass("groupNearCities", GroupNearByCity.class, DESC_GROUP_NEAR_CITIES);
		addClass("precission", Precission.class, DESC_PRECISSION);
		addClass("precissionCities", PrecissionByCity.class, DESC_PRECISSION_CITIES);
		addClass("load", Load.class, DESC_LOAD);
		addClass("cartodb", CartoDB.class, DESC_CARTODB);
	}

	/**
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
