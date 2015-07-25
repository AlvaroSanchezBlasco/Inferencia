package mapony.util.driver;

import mapony.inferencia.a_groupNear.total.MaponyGroupNearJob;
import mapony.inferencia.b_precision.MaponyPrecisionJob;
import mapony.inferencia.c_carga.MaponyCargaESJob;

/**
 * @author Alvaro Sanchez Blasco
 *
 */
public class Driver extends ProgramDriver {
	
	/**
	 * @throws Throwable
	 */
	public Driver() throws Throwable {
		super();
		addClass("1a", MaponyGroupNearJob.class, "Group By Near Places Job. Primer Job a ejecutar. Agrupa los datos por ciudades.");
		addClass("1b", MaponyPrecisionJob.class, "Job Precision. Agrupa con mayor precision los datos.");
		addClass("3", MaponyCargaESJob.class, "Carga de datos en ElasticSearch");
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
