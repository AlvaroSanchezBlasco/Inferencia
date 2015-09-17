package mapony.inferencia.util.cte;

import mapony.inferencia.util.Position;

/**
 * @author Alvaro Sanchez Blasco 
 * <p>Clase de constantes con las ciudades representativas que queremos procesar.
 */
public class CitiesCte {

	/**
	 * London
	 * <p>
	 * Lat. 51.50853 / Lon. -0.12574
	 * <p>
	 * GeoNameId : 2643743
	 */
//	public static final String posicionLondon = "51.50853,-0.12574";

	public static final Position london = new Position(new Double(51.50853), new Double(-0.12574));
	
	/**
	 * Madrid
	 * <p>
	 * Lat. 40.4165 / Lon. -3.70256
	 * <p>
	 * GeoNameId : 3117735
	 */
//	public static final String posicionMadrid = "40.4165,-3.70256";

	public static final Position madrid = new Position(new Double(40.4165), new Double(-3.70256));

	/**
	 * Berlin
	 * <p>
	 * Lat. 52.52437 / Lon. 13.41053
	 * <p>
	 * GeoNameId : 2950159
	 */
//	public static final String posicionBerlin = "52.52437,13.41053";
	
	public static final Position berlin = new Position(new Double(52.52437), new Double(13.41053));
	
	/**
	 * Roma
	 * <p>
	 * Lat. 41.90036 / Lon. 12.49575
	 * <p>
	 * GeoNameId : 3169071
	 */
//	public static final String posicionRoma = "41.90036,12.49575";

	public static final Position roma = new Position(new Double(41.90036), new Double(12.49575));
	
	/**
	 * Paris
	 * <p>
	 * Lat. 48.85341 / Lon. 2.3488
	 * <p>
	 * GeoNameId : 3169071
	 */
//	public static final String posicionParis="48.85341,2.3488";

	public static final Position paris = new Position(new Double(48.85341), new Double(2.3488));
	
	/**
	 * New York
	 * <p>
	 * Lat. 40.77427/ Lon. -73.96981
	 */
//	public static final String posicionNuevaYork="40.77427,-73.96981";

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
