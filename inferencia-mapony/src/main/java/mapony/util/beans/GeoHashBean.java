package mapony.util.beans;

import ch.hsr.geohash.GeoHash;
import mapony.util.constantes.MaponyCte;


/** @author Alvaro Sanchez Blasco
 *
 */
public class GeoHashBean {

	private String name;
	private double latitude;
	private double longitude;
	private String timezone;
	private String pais;
	private String continente;
	private String geoHash;
	
	/**
	 * Constructor sin parametros
	 */
	public GeoHashBean() {
	}


	/**
	 * @param geonameid
	 * @param name
	 * @param asciiname
	 * @param alternatenames
	 * @param latitude
	 * @param longitude
	 * @param featureclass
	 * @param featurecode
	 * @param countrycode
	 * @param cc2
	 * @param admin1code
	 * @param admin2code
	 * @param admin3code
	 * @param admin4code
	 * @param population
	 * @param elevation
	 * @param dem
	 * @param timezone
	 * @param modificationdate
	 * @param precisionGeoHash
	 */
	public GeoHashBean(final String geonameid, final String name, final String asciiname, final String alternatenames, final String latitude,
			final String longitude, String featureclass, final String featurecode, final String countrycode, final String cc2,
			final String admin1code, final String admin2code, final String admin3code, String admin4code, final String population,
			final String elevation, final String dem, final String timezone, final String modificationdate, final int precisionGeoHash) {
		setName(name);
		setLatitude(new Double(latitude));
		setLongitude(new Double(longitude));
		setTimezone(timezone);
		String[] continentePais = timezone.split("/");
		setContinente(continentePais[0]);
		setPais(continentePais[1]);
		setGeoHash(GeoHash.geoHashStringWithCharacterPrecision(getLatitude(), getLongitude(), precisionGeoHash));
	}

	/**
	 * Construye un GeoHashBean con los datos de String[] que recibe como parametro
	 * 
	 * @param datos
	 * @param precisionGeoHash
	 */
	public GeoHashBean(final String[] datos, int precisionGeoHash) {
		setName(datos[1]);
		setLatitude(new Double(datos[4]));
		setLongitude(new Double(datos[5]));
		setTimezone(datos[17]);
		if (null != getTimezone() && getTimezone().compareTo(MaponyCte.VACIO) != 0) {
			try {
				String[] continentePais = getTimezone().split("/");
				if (null != continentePais[0] && null != continentePais[1]) {
					setContinente(continentePais[0]);
					setPais(continentePais[1]);
					setGeoHash(GeoHash.geoHashStringWithCharacterPrecision(getLatitude(), getLongitude(), precisionGeoHash));
				}
			} catch (Exception e) {
				throw e;
			}
		}
	}

	/**
	 * @return the name
	 */
	public final String getName() {
		return name;
	}


	/**
	 * @param name the name to set
	 */
	private final void setName(final String name) {
		this.name = name;
	}

	/**
	 * @return the timezone
	 */
	private final String getTimezone() {
		return timezone;
	}


	/**
	 * @param timezone the timezone to set
	 */
	private final void setTimezone(final String timezone) {
		this.timezone = timezone;
	}

	/**
	 * @return the pais
	 */
	public final String getPais() {
		return pais;
	}


	/**
	 * @param pais the pais to set
	 */
	private final void setPais(final String pais) {
		this.pais = pais;
	}


	/**
	 * @return the continente
	 */
	public final String getContinente() {
		return continente;
	}


	/**
	 * @param continente the continente to set
	 */
	private final void setContinente(final String continente) {
		this.continente = continente;
	}


	/**
	 * @return the latitude
	 */
	private final double getLatitude() {
		return latitude;
	}


	/**
	 * @param latitude the latitude to set
	 */
	private final void setLatitude(final double latitude) {
		this.latitude = latitude;
	}


	/**
	 * @return the longitude
	 */
	private final double getLongitude() {
		return longitude;
	}


	/**
	 * @param longitude the longitude to set
	 */
	private final void setLongitude(final double longitude) {
		this.longitude = longitude;
	}


	/**
	 * @return the geoHash
	 */
	public final String getGeoHash() {
		return geoHash;
	}


	/**
	 * @param geoHash the geoHash to set
	 */
	private final void setGeoHash(final String geoHash) {
		this.geoHash = geoHash;
	}
}
