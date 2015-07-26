package mapony.util.driver;

import mapony.city.a_groupNear.MaponyGroupNearByCityJob;
import mapony.city.b_precission.MaponyCityPrecissionJob;
import mapony.common.jobs.load.MaponyCargaESJob;
import mapony.inferencia.a_groupNear.MaponyGroupNearJob;
import mapony.inferencia.b_precission.MaponyPrecissionJob;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Clase para controlar las ejecuciones de los Job.
 *         <p>
 *         Class to control the Job's executions.
 */
public class Driver extends ProgramDriver {

	/**
	 * Descripcion para el Job groupNear
	 * <p>
	 * GroupNear Job's description
	 * <p>
	 * Group By Near Places Job.
	 * <p>
	 * Primer Job a ejecutar. Agrupa los datos por ciudades.
	 * <p>
	 * First Job to run. Arrange data by populated cities.
	 */
	private final String DESC_GROUP_NEAR = "\tGroup By Near Places Job. \n\t\tPrimer Job a ejecutar. Agrupa los datos por ciudades.\n\t\tFirst Job to run. Arrange data by populated cities.";

	/**
	 * Descripcion para el Job groupNear
	 * <p>
	 * GroupNear Job's description
	 * <p>
	 * Group By Near Places Job.
	 * <p>
	 * Primer Job a ejecutar. Agrupa los datos por ciudades.
	 * <p>
	 * First Job to run. Arrange data by populated cities.
	 */
	private final String DESC_GROUP_NEAR_CITIES = "\tGroup By Near Places Selected Cities Job. \n\t\tPrimer Job a ejecutar. Agrupa los datos de las ciudades: Madrid, Londres, Berlin, Roma, Paris, Nueva York.\n\t\tFirst Job to run. Arrange data by the cities: Madrid, London, Berlin, Rome, Paris, New York.";

	/**
	 * Descripcion para el Job precission
	 * <p>
	 * Precission Job's description
	 * <p>
	 * Precission Job.
	 * <p>
	 * Segundo Job a ejecutar. Agrupa con mayor precision los datos.
	 * <p>
	 * Second Job to run. Gives more accuracy to arrange data.
	 */
	private final String DESC_PRECISSION = "\tPrecission Job. \n\t\tSegundo Job a ejecutar. Agrupa con mayor precision los datos.\n\t\tSecond Job to run. Gives more accuracy to arrange data.";

	/**
	 * Descripcion para el Job precission
	 * <p>
	 * Precission Job's description
	 * <p>
	 * Precission Job.
	 * <p>
	 * Segundo Job a ejecutar. Agrupa con mayor precision los datos.
	 * <p>
	 * Second Job to run. Gives more accuracy to arrange data.
	 */
	private final String DESC_PRECISSION_CITIES = "\tPrecission Job. \n\t\tSegundo Job a ejecutar. Agrupa con mayor precision los de las ciudades: Madrid, Londres, Berlin, Roma, Paris, Nueva York.\n\t\tSecond Job to run. Gives more accuracy to arrange data of the cities: Madrid, London, Berlin, Rome, Paris, New York.";

	/**
	 * Descripcion para el Job load
	 * <p>
	 * Load Job's description
	 * <p>
	 * Load Job.
	 * <p>
	 * Tercer Job a ejecutar. Carga los datos en un cluster de ElasticSearch.
	 * <p>
	 * Third Job to run. Loads data into a ElasticSearch cluster.
	 */
	private final String DESC_LOAD = "\tLoad Job. \n\t\tTercer Job a ejecutar. Carga los datos en un cluster de ElasticSearch.\n\t\tThird Job to run. Loads data into a ElasticSearch cluster.";

	/**
	 * @throws Throwable
	 */
	public Driver() throws Throwable {
		super();
		addClass("groupNear", MaponyGroupNearJob.class, DESC_GROUP_NEAR);
		addClass("groupNearCities", MaponyGroupNearByCityJob.class, DESC_GROUP_NEAR_CITIES);
		addClass("precission", MaponyPrecissionJob.class, DESC_PRECISSION);
		addClass("precissionCities", MaponyCityPrecissionJob.class, DESC_PRECISSION_CITIES);
		addClass("load", MaponyCargaESJob.class, DESC_LOAD);
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
