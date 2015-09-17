// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mapony.inferencia.geoHash.bean.GeoHashBean;
import mapony.inferencia.util.cte.InferenciaCte;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Alvaro Sanchez Blasco
 *         <ul>
 *         <li>[ES] CustomWritable para procesar los datos del dataset de Flickr.
 *         <li>[EN] CustomWritable to process the data of the Flick's dataset.
 *         </ul>
 */
public class RawData implements WritableComparable<RawData> {

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

	public RawData() {
		set();
	}

	public RawData(final String[] data, final GeoHashBean ghb) {
		set(data, ghb);
	}

	public RawData(final String[] data, final String geoHash) {
		set(data, geoHash);
	}
	
	public RawData(final RawData rd) {
		set(rd);
	}

	public RawData(final RawData rd, final String newGeoHash) {
		set(rd, newGeoHash);
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
		result = prime * result + ((longitude == null) ? 0 : longitude.hashCode());
		result = prime * result + ((latitude == null) ? 0 : latitude.hashCode());
		return result;
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

	/**
	 * @param identifier
	 * @param dateTaken
	 * @param captureDevice
	 * @param title
	 * @param description
	 * @param userTags
	 * @param machineTags
	 * @param longitude
	 * @param latitude
	 * @param downloadUrl
	 * @param geoHash
	 * @param ciudad
	 */
	private void set(final String identifier, final String dateTaken, final String captureDevice, final String title, final String description, final String userTags,
			final String machineTags, final String longitude, final String latitude, final String downloadUrl, final String geoHash, final String ciudad) {
		this.identifier = new Text(identifier);
		this.dateTaken = new Text(dateTaken);
		this.captureDevice = new Text(captureDevice);
		this.title = new Text(title);
		this.description = new Text(description);
		this.userTags = new Text(userTags);
		this.machineTags = new Text(machineTags);
		this.longitude = new Text(longitude);
		this.latitude = new Text(latitude);
		this.downloadUrl = new Text(downloadUrl);
		this.geoHash = new Text(geoHash);
		this.ciudad = new Text(ciudad);
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
		this.geoHash = new Text(rd.getGeoHash().toString());
		this.ciudad = new Text(rd.getCiudad().toString());
	}

	private void set(final RawData rd, final String newGeoHash) {
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
		this.geoHash = new Text(newGeoHash);
		this.ciudad = new Text(rd.getCiudad().toString());
	}

	private void set(final String[] data, final GeoHashBean ghb){
		set(data[0], data[3], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[14], ghb.getGeoHash(), ghb.getCity());
	}

	private void set(final String[] data, final String geoHash){
		set(data[0], data[3], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[14], geoHash, InferenciaCte.EMPTY_STRING);
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

	public int compareTo(RawData o) {
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
	public boolean equals(RawData o) {
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