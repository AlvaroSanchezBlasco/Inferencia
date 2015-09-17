package mapony.inferencia.geoHash.util;

import java.util.HashMap;

import mapony.inferencia.geoHash.bean.GeoHashBean;
import mapony.inferencia.util.cte.CitiesCte;
import mapony.inferencia.util.exception.InferenciaException;

/**
 * @author Alvaro Sanchez Blasco
 * 
 */
public class GeoHashCiudad {

	private static HashMap<String, GeoHashBean> dataFromGeonames;
	private static HashMap<String, GeoHashBean> dataFromSelectedCities;

	public GeoHashCiudad(final int precision) {
		loadSelectedCities(precision);
	}

	/**
	 * @param cadena
	 * @param precisionGeoHash
	 * @throws InferenciaException 
	 * @throws Exception
	 */
	public static void cargaHashDatosCiudades(String cadena, final int precisionGeoHash) throws InferenciaException {
		GeoHashBean bean;
		try {
			bean = new GeoHashBean(cadena.split("\t"), precisionGeoHash);
			String geoHashString = bean.getGeoHash();
			getDatos().put(geoHashString, bean);
		} catch (Exception e) {
			throw new InferenciaException(e, e.getMessage());
		}
	}

	/**
	 * Carga un HashMap con los datos de las ciudades seleccionadas:
	 * <ul>
	 * <li>Madrid.
	 * <li>Londres.
	 * <li>Berlin.
	 * <li>Roma.
	 * <li>Paris.
	 * <li>Nueva York.
	 * </ul>
	 * 
	 * @param precision
	 *            del geoHash a calcular
	 * @return HashMap con los datos de las ciudades seleccionadas.
	 */
	public static HashMap<String, GeoHashBean> loadSelectedCities(final int precision) {
		dataFromSelectedCities = new HashMap<String, GeoHashBean>();

		GeoHashBean cghLondres = new GeoHashBean(CitiesCte.sLondres, CitiesCte.london, precision);
		dataFromSelectedCities.put(cghLondres.getGeoHash(), cghLondres);

		GeoHashBean cghMadrid = new GeoHashBean(CitiesCte.sMadrid, CitiesCte.madrid, precision);
		dataFromSelectedCities.put(cghMadrid.getGeoHash(), cghMadrid);

		GeoHashBean cghBerlin = new GeoHashBean(CitiesCte.sBerlin, CitiesCte.berlin, precision);
		dataFromSelectedCities.put(cghBerlin.getGeoHash(), cghBerlin);

		GeoHashBean cghRoma = new GeoHashBean(CitiesCte.sRoma, CitiesCte.roma, precision);
		dataFromSelectedCities.put(cghRoma.getGeoHash(), cghRoma);

		GeoHashBean cghParis = new GeoHashBean(CitiesCte.sParis, CitiesCte.paris, precision);
		dataFromSelectedCities.put(cghParis.getGeoHash(), cghParis);

		GeoHashBean cghNuevaYork = new GeoHashBean(CitiesCte.sNuevaYork, CitiesCte.ny, precision);
		dataFromSelectedCities.put(cghNuevaYork.getGeoHash(), cghNuevaYork);

		return dataFromSelectedCities;
	}

	/**
	 * @return the datos
	 * @throws Exception
	 */
	public static final HashMap<String, GeoHashBean> getDatos() {
		if (null == dataFromGeonames) {
			dataFromGeonames = new HashMap<String, GeoHashBean>();
		}
		return dataFromGeonames;
	}

	/**
	 * @return the datos
	 * @throws Exception
	 */
	public static final HashMap<String, GeoHashBean> getDatosCiudadesSeleccionadas() {
		if (null == dataFromSelectedCities) {
			dataFromSelectedCities = new HashMap<String, GeoHashBean>();
		}
		return dataFromSelectedCities;
	}
}
