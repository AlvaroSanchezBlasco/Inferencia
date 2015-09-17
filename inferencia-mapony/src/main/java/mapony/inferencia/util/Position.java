package mapony.inferencia.util;

public class Position {

	private Double longitude;
	private Double latitude;
	
	public Position(final String[] dato){
		setLongitude(new Double(dato[10]));
		setLatitude(new Double(dato[11]));
	}
	
	/**
	 * @param latitude
	 * @param longitude
	 */
	public Position(final Double latitude, final Double longitude){
		setLongitude(longitude);
		setLatitude(latitude);
	}
	
	public final Double getLongitude() {
		return longitude;
	}
	public final void setLongitude(Double longitude) {
		this.longitude = longitude;
	}
	public final Double getLatitude() {
		return latitude;
	}
	public final void setLatitude(Double latitude) {
		this.latitude = latitude;
	}
	
}
