// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.util.cte;

/**
 * @author Alvaro Sanchez Blasco
 *         <ul>
 *         <li>[ES] Clase de constantes
 *         <li>[EN] Constants
 *         </ul>
 */
public final class InferenciaCte {

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
	 * ''
	 */
	public static final String EMPTY_STRING = "";
	
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
	public static final String COMMA = ",";

	public static final int FAIL = -1;
	
	public static final int SUCCESS = 0;
	
	public static final int JOB_COMPLETION_FAILED = 1;
	
	public static final String VIDEO_FORMAT = "1";
	
	public static final String PHOTO_FORMAT = "0";
	
	public static final int NUM_ARGS = 1;
	
	public static final String NO_MATTER_WHAT_CITY = "indiferente";
	
	public static final String hdfsUri = "hdfs://quickstart.cloudera:8020/";
}
