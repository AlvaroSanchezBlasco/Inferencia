// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import mapony.inferencia.util.cte.InferenciaCte;


/**
 * @author Alvaro Sanchez Blasco
 *         <ul>
 *         <li>[ES] CustomWritable para procesar los datos del dataset de Flickr.
 *         <li>[EN] CustomWritable to process the data of the Flick's dataset.
 *         </ul>
 */
public class CartoDb implements WritableComparable<CartoDb> {

	/**
	 * Photo/Video Identifier
	 */
	private Text identifier;
	/**
	 * Date Taken
	 */
	private Text dateTaken;
	/**
	 * Capture Device
	 */
	private Text captureDevice;
	/**
	 * Title
	 */
	private Text title;
	/**
	 * Description
	 */
	private Text description;
	/**
	 * User Tags (Comma-Separated)
	 */
	private Text userTags;
	/**
	 * Machine Tags (Comma-Separated)
	 */
	private Text machineTags;
	/**
	 * Longitude
	 */
	private Text longitude;
	/**
	 * Latitude
	 */
	private Text latitude;
	/**
	 * Photo/Video Download Url
	 */
	private Text downloadUrl;

	/**
	 * GeoHas calculado
	 */
	private Text geoHash;

	/**
	 * Ciudad. relacionado con el geohash y el registro.
	 * <p>
	 * City. Related with the geohash and the record processed.
	 */
	private Text ciudad;

	public CartoDb() {
		set();
	}

	public CartoDb(final RawData rd) {
		set(rd);
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
		result = prime * result + ((longitude == null) ? 0 : longitude.hashCode());
		result = prime * result + ((latitude == null) ? 0 : latitude.hashCode());
		return result;
	}

	public String toString() {
		StringBuilder sbRetorno = new StringBuilder();

		sbRetorno.append(getIdentifier().toString()).append(InferenciaCte.COMMA);
		sbRetorno.append(getDateTaken().toString()).append(InferenciaCte.COMMA);
		sbRetorno.append(getLongitude().toString()).append(InferenciaCte.COMMA);
		sbRetorno.append(getCaptureDevice().toString()).append(InferenciaCte.COMMA);
		sbRetorno.append(getLatitude().toString());
		
		return sbRetorno.toString();
	}
	
	private void set() {
		this.identifier = new Text();
		this.dateTaken = new Text();
		this.captureDevice = new Text();
		this.title = new Text();
		this.description = new Text();
		this.userTags = new Text();
		this.machineTags = new Text();
		this.longitude = new Text();
		this.latitude = new Text();
		this.downloadUrl = new Text();
		this.geoHash = new Text();
		this.ciudad = new Text();
	}

	private void set(final RawData rd) {
		this.identifier = new Text(rd.getIdentifier().toString());
		this.dateTaken = new Text(rd.getDateTaken().toString());
		this.captureDevice = new Text(rd.getCaptureDevice().toString());
		this.title = new Text(rd.getTitle().toString());
		this.description = new Text(rd.getDescription().toString());
		this.userTags = new Text(rd.getUserTags().toString());
		this.machineTags = new Text(rd.getMachineTags().toString());
		this.longitude = new Text(rd.getLongitude().toString());
		this.latitude = new Text(rd.getLatitude().toString());
		this.downloadUrl = new Text(rd.getDownloadUrl().toString());
		this.geoHash = new Text(rd.getGeoHash());
		this.ciudad = new Text(rd.getCiudad().toString());
	}

	public void write(DataOutput out) throws IOException {
		identifier.write(out);
		dateTaken.write(out);
		captureDevice.write(out);
		title.write(out);
		description.write(out);
		userTags.write(out);
		machineTags.write(out);
		longitude.write(out);
		latitude.write(out);
		downloadUrl.write(out);
		geoHash.write(out);
		ciudad.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		identifier.readFields(in);
		dateTaken.readFields(in);
		captureDevice.readFields(in);
		title.readFields(in);
		description.readFields(in);
		userTags.readFields(in);
		machineTags.readFields(in);
		longitude.readFields(in);
		latitude.readFields(in);
		downloadUrl.readFields(in);
		geoHash.readFields(in);
		ciudad.readFields(in);
	}

	public int compareTo(CartoDb o) {
		return identifier.compareTo(o.getIdentifier());
	}

	/**
	 * Compara dos RawData por el campo que los identifica univocamente, el identifier
	 * <p>
	 * Compares two Instances of a RawData by it's identifier (must be unique).
	 * 
	 * @param o
	 * @return true / false si los Objetos son iguales o no.
	 */
	public boolean equals(CartoDb o) {
		return identifier.equals(o.getIdentifier());
	}

	public final Text getIdentifier() {
		return identifier;
	}

	public final void setIdentifier(Text identifier) {
		this.identifier = identifier;
	}

	public final Text getDateTaken() {
		return dateTaken;
	}

	public final void setDateTaken(Text dateTaken) {
		this.dateTaken = dateTaken;
	}

	public final Text getCaptureDevice() {
		return captureDevice;
	}

	public final void setCaptureDevice(Text captureDevice) {
		this.captureDevice = captureDevice;
	}

	public final Text getTitle() {
		return title;
	}

	public final void setTitle(Text title) {
		this.title = title;
	}

	public final Text getDescription() {
		return description;
	}

	public final void setDescription(Text description) {
		this.description = description;
	}

	public final Text getUserTags() {
		return userTags;
	}

	public final void setUserTags(Text userTags) {
		this.userTags = userTags;
	}

	public final Text getMachineTags() {
		return machineTags;
	}

	public final void setMachineTags(Text machineTags) {
		this.machineTags = machineTags;
	}

	public final Text getLongitude() {
		return longitude;
	}

	public final void setLongitude(Text longitude) {
		this.longitude = longitude;
	}

	public final Text getLatitude() {
		return latitude;
	}

	public final void setLatitude(Text latitude) {
		this.latitude = latitude;
	}

	public final Text getDownloadUrl() {
		return downloadUrl;
	}

	public final void setDownloadUrl(Text downloadUrl) {
		this.downloadUrl = downloadUrl;
	}

	public final Text getGeoHash() {
		return geoHash;
	}

	public final void setGeoHash(Text geoHash) {
		this.geoHash = geoHash;
	}

	public final Text getCiudad() {
		return ciudad;
	}

	public final void setCiudad(Text ciudad) {
		this.ciudad = ciudad;
	}
}