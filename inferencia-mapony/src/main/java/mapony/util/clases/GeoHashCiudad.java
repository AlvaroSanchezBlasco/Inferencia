package mapony.util.clases;

import java.io.IOException;
import java.util.HashMap;

import mapony.util.beans.CustomGeoHashBean;
import mapony.util.beans.GeoHashBean;
import mapony.util.constantes.MaponyCiudadesCte;

/**
 * @author Alvaro Sanchez Blasco
 *
 */
public class GeoHashCiudad {

	private static HashMap<String, GeoHashBean> datos;
	private static HashMap<String, CustomGeoHashBean> datosCiudadesSeleccionadas;

	public GeoHashCiudad(final int precision){
		cargaGeoHashCiudadesSeleccionadas(precision);
	}

	/**
	 * Lee el fichero de ciudades, y por cada registro, cargar en mi HashMap los datos correspondientes.
	 * @throws IOException 
	 */
	public HashMap<String, GeoHashBean> cargaGeoHashCiudades(String cadena, final int precisionGeoHash) throws IOException {
		// Lee el fichero de ciudades, y por cada registro, cargar en mi HashMap los datos
		// correspondientes.
		GeoHashBean bean = new GeoHashBean(cadena.split("\t"), precisionGeoHash);
		String geoHashString = bean.getGeoHash();
		datos.put(geoHashString, bean);

		return datos;
	}

	/**
	 * @param cadena
	 * @param precisionGeoHash
	 * @throws Exception
	 */
	public static void cargaHashDatosCiudades(String cadena, final int precisionGeoHash) throws Exception {
		GeoHashBean bean = new GeoHashBean(cadena.split("\t"), precisionGeoHash);
		String geoHashString = bean.getGeoHash();
		getDatos().put(geoHashString, bean);
	}
	
	/**
	 * Carga un HashMap con los datos de las ciudades seleccionadas:<p>
	 * - Madrid.<p>
	 * - Londres.<p>
	 * - Berlin.<p>
	 * - Roma. <p>
	 * 
	 * @param precision del geoHash a calcular
	 * @return HashMap con los datos de las ciudades seleccionadas.
	 */
	public static HashMap<String, CustomGeoHashBean> cargaGeoHashCiudadesSeleccionadas(final int precision) {
		datosCiudadesSeleccionadas = new HashMap<String, CustomGeoHashBean>();

		CustomGeoHashBean cghLondres = new CustomGeoHashBean(MaponyCiudadesCte.sLondres, MaponyCiudadesCte.posicionLondon, precision);
		datosCiudadesSeleccionadas.put(cghLondres.getGeoHash(), cghLondres);

		CustomGeoHashBean cghMadrid = new CustomGeoHashBean(MaponyCiudadesCte.sMadrid, MaponyCiudadesCte.posicionMadrid, precision);
		datosCiudadesSeleccionadas.put(cghMadrid.getGeoHash(), cghMadrid);

		CustomGeoHashBean cghBerlin = new CustomGeoHashBean(MaponyCiudadesCte.sBerlin, MaponyCiudadesCte.posicionBerlin, precision);
		datosCiudadesSeleccionadas.put(cghBerlin.getGeoHash(), cghBerlin);

		CustomGeoHashBean cghRoma = new CustomGeoHashBean(MaponyCiudadesCte.sRoma, MaponyCiudadesCte.posicionRoma, precision);
		datosCiudadesSeleccionadas.put(cghRoma.getGeoHash(), cghRoma);

		CustomGeoHashBean cghParis = new CustomGeoHashBean(MaponyCiudadesCte.sParis, MaponyCiudadesCte.posicionParis, precision);
		datosCiudadesSeleccionadas.put(cghParis.getGeoHash(), cghParis);
		
		CustomGeoHashBean cghNuevaYork = new CustomGeoHashBean(MaponyCiudadesCte.sNuevaYork, MaponyCiudadesCte.posicionNuevaYork, precision);
		datosCiudadesSeleccionadas.put(cghNuevaYork.getGeoHash(), cghNuevaYork);

		return datosCiudadesSeleccionadas;
	}
 
	/**
	 * @return the datos
	 * @throws Exception
	 */
	public static final HashMap<String, GeoHashBean> getDatos() throws Exception {
		if (null == datos) {
			datos = new HashMap<String, GeoHashBean>();
		}
		return datos;
	}
	
	/**
	 * @return the datos
	 * @throws Exception
	 */
	public static final HashMap<String, CustomGeoHashBean> getDatosCiudadesSeleccionadas() throws Exception {
		if (null == datosCiudadesSeleccionadas) {
			throw new Exception("Datos de ciudades no cargados");
		}
		return datosCiudadesSeleccionadas;
	}
}
