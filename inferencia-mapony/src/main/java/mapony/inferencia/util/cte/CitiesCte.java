package mapony.inferencia.util.cte;

import mapony.inferencia.util.Position;

/**
 * @author Alvaro Sanchez Blasco 
 * <p>Clase de constantes con las ciudades representativas que queremos procesar.
 */
public class CitiesCte {

	/**
	 * London — Lat. 51.50853 / Lon. -0.12574 — GeoNameId: 2643743
	 */
	public static final Position london = new Position(new Double(51.50853), new Double(-0.12574));

	/**
	 * Madrid — Lat. 40.4165 / Lon. -3.70256 — GeoNameId: 3117735
	 */
	public static final Position madrid = new Position(new Double(40.4165), new Double(-3.70256));

	/**
	 * Berlin — Lat. 52.52437 / Lon. 13.41053 — GeoNameId: 2950159
	 */
	public static final Position berlin = new Position(new Double(52.52437), new Double(13.41053));

	/**
	 * Roma — Lat. 41.90036 / Lon. 12.49575 — GeoNameId: 3169071
	 */
	public static final Position roma = new Position(new Double(41.90036), new Double(12.49575));

	/**
	 * Paris — Lat. 48.85341 / Lon. 2.3488 — GeoNameId: 3169071
	 */
	public static final Position paris = new Position(new Double(48.85341), new Double(2.3488));

	/**
	 * New York — Lat. 40.77427 / Lon. -73.96981
	 */
	public static final Position ny = new Position(new Double(40.77427), new Double(-73.96981));

	/**
	 * Londres
	 */
	public static final String sLondres = "Londres";
	/**
	 * Madrid
	 */
	public static final String sMadrid = "Madrid";
	/**
	 * Berlin
	 */
	public static final String sBerlin = "Berlin";
	/**
	 * Roma
	 * */
	public static final String sRoma = "Roma";
	/**
	 * Paris
	 * */
	public static final String sParis = "Paris";
	/**
	 * Nueva York
	 * */
	public static final String sNuevaYork = "NY";
}
