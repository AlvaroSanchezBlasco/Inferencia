package mapony.inferencia.geoHash.bean;

import mapony.inferencia.util.Position;
import ch.hsr.geohash.GeoHash;


/** @author Alvaro Sanchez Blasco
 *
 */
public class GeoHashBean {

	private String city;
	private String geoHash;
	private double latitude;
	private double longitude;

	public GeoHashBean() {
	}

	@Deprecated
	public GeoHashBean(final String[] datos, final int precisionGeoHash) throws Exception {
		setCity(datos[1]);
		setLatitude(new Double(datos[4]));
		setLongitude(new Double(datos[5]));
		setGeoHash(GeoHash.geoHashStringWithCharacterPrecision(getLatitude(), getLongitude(), precisionGeoHash));
	}

	public GeoHashBean(final String name, final Position posicion, final int precision) {
		setCity(name);
		setLatitude(posicion.getLatitude());
		setLongitude(posicion.getLongitude());
		setGeoHash(GeoHash.geoHashStringWithCharacterPrecision(getLatitude(), getLongitude(), precision));
	}
	
	public final String getCity() {
		return city;
	}
	public final void setCity(String city) {
		this.city = city;
	}
	public final String getGeoHash() {
		return geoHash;
	}
	public final void setGeoHash(String geoHash) {
		this.geoHash = geoHash;
	}
	
	private final double getLatitude() {
		return latitude;
	}

	private final void setLatitude(final double latitude) {
		this.latitude = latitude;
	}

	private final double getLongitude() {
		return longitude;
	}

	private final void setLongitude(final double longitude) {
		this.longitude = longitude;
	}
}
