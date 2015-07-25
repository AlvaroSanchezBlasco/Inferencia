package mapony.util.beans;

import ch.hsr.geohash.GeoHash;
import mapony.util.constantes.MaponyCte;


/** @author Alvaro Sanchez Blasco
 *
 */
public class CustomGeoHashBean {

	private String name;
	private double latitude;
	private double longitude;
	private String geoHash;
	
	/**
	 * Constructor sin parámetros
	 */
	public CustomGeoHashBean() {
	}


	/**
	 * @param name
	 * @param posicion
	 */
	public CustomGeoHashBean(final String name, final String posicion, final int precision) {
		setName(name);
		String[] pos = posicion.split(MaponyCte.COMA);
		setLatitude(new Double(pos[0]));
		setLongitude(new Double(pos[1]));
		setGeoHash(GeoHash.geoHashStringWithCharacterPrecision(getLatitude(), getLongitude(), precision));
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
