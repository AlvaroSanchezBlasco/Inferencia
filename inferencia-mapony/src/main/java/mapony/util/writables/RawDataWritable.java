package mapony.util.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         CustomWritable para procesar los datos del dataset de Flickr.
 *         <p>
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
 * @param continente
 * @param pais
 * @param ciudad
 */
public class RawDataWritable implements WritableComparable<RawDataWritable> {

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
	 * Pais relacionado con el geohash y el registro
	 */
	private Text pais;
	/**
	 * Ciudad relacionado con el geohash y el registro
	 */
	private Text ciudad;
	/**
	 * Continente relacionado con el geohash y el registro
	 */
	private Text continente;

	/**
	 * Constructor sin parametros.
	 */
	public RawDataWritable() {
		set(new Text(), new Text(), new Text(), new Text(), new Text(), new Text(), new Text(), new Text(), new Text(),
				new Text(), new Text(), new Text(), new Text(), new Text());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
		result = prime * result + ((longitude == null) ? 0 : longitude.hashCode());
		result = prime * result + ((latitude == null) ? 0 : latitude.hashCode());
		return result;
	}

	/**
	 * Constructor con parámetros
	 * 
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
	 * @param continente
	 * @param pais
	 * @param ciudad
	 */
	public RawDataWritable(Text identifier, Text dateTaken, Text captureDevice, Text title, Text description,
			Text userTags, Text machineTags, Text longitude, Text latitude, Text downloadUrl, Text geoHash,
			Text continente, Text pais, Text ciudad) {
		set(new Text(identifier.toString()), new Text(dateTaken.toString()), new Text(captureDevice.toString()),
				new Text(title.toString()), new Text(description.toString()), new Text(userTags.toString()),
				new Text(machineTags.toString()), new Text(longitude.toString()), new Text(latitude.toString()),
				new Text(downloadUrl.toString()), new Text(geoHash.toString()), new Text(continente.toString()),
				new Text(pais.toString()), new Text(ciudad.toString()));

	}

	/**
	 * Inicializa los campos del writable con los parametros recibidos.
	 * 
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
	 * @param continente
	 * @param pais
	 * @param ciudad
	 */
	private void set(Text identifier, Text dateTaken, Text captureDevice, Text title, Text description, Text userTags,
			Text machineTags, Text longitude, Text latitude, Text downloadUrl, Text geoHash, Text continente, Text pais,
			Text ciudad) {
		this.identifier = new Text(identifier.toString());
		this.dateTaken = new Text(dateTaken.toString());
		this.captureDevice = new Text(captureDevice.toString());
		this.title = new Text(title.toString());
		this.description = new Text(description.toString());
		this.userTags = new Text(userTags.toString());
		this.machineTags = new Text(machineTags.toString());
		this.longitude = new Text(longitude.toString());
		this.latitude = new Text(latitude.toString());
		this.downloadUrl = new Text(downloadUrl.toString());
		this.geoHash = new Text(geoHash.toString());
		this.continente = new Text(continente.toString());
		this.pais = new Text(pais.toString());
		this.ciudad = new Text(ciudad.toString());
	}

	/**
	 * @param rdw
	 */
	public RawDataWritable(final RawDataWritable rdw) {
		this.identifier = new Text(rdw.getIdentifier().toString());
		this.dateTaken = new Text(rdw.getDateTaken().toString());
		this.captureDevice = new Text(rdw.getCaptureDevice().toString());
		this.title = new Text(rdw.getTitle().toString());
		this.description = new Text(rdw.getDescription().toString());
		this.userTags = new Text(rdw.getUserTags().toString());
		this.machineTags = new Text(rdw.getMachineTags().toString());
		this.longitude = new Text(rdw.getLongitude().toString());
		this.latitude = new Text(rdw.getLatitude().toString());
		this.downloadUrl = new Text(rdw.getDownloadUrl().toString());
		this.geoHash = new Text(rdw.getGeoHash().toString());
		this.continente = new Text(rdw.getContinente().toString());
		this.pais = new Text(rdw.getPais().toString());
		this.ciudad = new Text(rdw.getCiudad().toString());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
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
		continente.write(out);
		pais.write(out);
		ciudad.write(out);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
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
		continente.readFields(in);
		pais.readFields(in);
		ciudad.readFields(in);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(RawDataWritable o) {
		return identifier.compareTo(o.identifier);
	}

	/**
	 * Compara dos RawDataWritable por el campo que los identifica unívocamente, el identifier
	 * 
	 * @param o
	 * @return true / false si los Objetos son iguales o no. 
	 */
	public boolean equals(RawDataWritable o) {
		return identifier.equals(o.identifier);
	}

	/**
	 * @return the identifier
	 */
	public final Text getIdentifier() {
		return identifier;
	}

	/**
	 * @param identifier
	 *            the identifier to set
	 */
	public final void setIdentifier(Text identifier) {
		this.identifier = new Text(identifier.toString());
	}

	/**
	 * @return the dateTaken
	 */
	public final Text getDateTaken() {
		return dateTaken;
	}

	/**
	 * @param dateTaken
	 *            the dateTaken to set
	 */
	public final void setDateTaken(Text dateTaken) {
		this.dateTaken = new Text(dateTaken.toString());
	}

	/**
	 * @return the captureDevice
	 */
	public final Text getCaptureDevice() {
		return captureDevice;
	}

	/**
	 * @param captureDevice
	 *            the captureDevice to set
	 */
	public final void setCaptureDevice(Text captureDevice) {
		this.captureDevice = new Text(captureDevice.toString());
	}

	/**
	 * @return the title
	 */
	public final Text getTitle() {
		return title;
	}

	/**
	 * @param title
	 *            the title to set
	 */
	public final void setTitle(Text title) {
		this.title = new Text(title.toString());
	}

	/**
	 * @return the description
	 */
	public final Text getDescription() {
		return description;
	}

	/**
	 * @param description
	 *            the description to set
	 */
	public final void setDescription(Text description) {
		this.description = new Text(description.toString());
	}

	/**
	 * @return the userTags
	 */
	public final Text getUserTags() {
		return userTags;
	}

	/**
	 * @param userTags
	 *            the userTags to set
	 */
	public final void setUserTags(Text userTags) {
		this.userTags = new Text(userTags.toString());
	}

	/**
	 * @return the machineTags
	 */
	public final Text getMachineTags() {
		return machineTags;
	}

	/**
	 * @param machineTags
	 *            the machineTags to set
	 */
	public final void setMachineTags(Text machineTags) {
		this.machineTags = new Text(machineTags.toString());
	}

	/**
	 * @return the longitude
	 */
	public final Text getLongitude() {
		return longitude;
	}

	/**
	 * @param longitude
	 *            the longitude to set
	 */
	public final void setLongitude(Text longitude) {
		this.longitude = new Text(longitude.toString());
	}

	/**
	 * @return the latitude
	 */
	public final Text getLatitude() {
		return latitude;
	}

	/**
	 * @param latitude
	 *            the latitude to set
	 */
	public final void setLatitude(Text latitude) {
		this.latitude = new Text(latitude.toString());
	}

	/**
	 * @return the downloadUrl
	 */
	public final Text getDownloadUrl() {
		return downloadUrl;
	}

	/**
	 * @param downloadUrl
	 *            the downloadUrl to set
	 */
	public final void setDownloadUrl(Text downloadUrl) {
		this.downloadUrl = new Text(downloadUrl.toString());
	}

	/**
	 * @return the geoHash
	 */
	public final Text getGeoHash() {
		return geoHash;
	}

	/**
	 * @param geoHash
	 *            the geoHash to set
	 */
	public final void setGeoHash(Text geoHash) {
		this.geoHash = new Text(geoHash.toString());
	}

	/**
	 * @return the pais
	 */
	public final Text getPais() {
		return pais;
	}

	/**
	 * @param pais
	 *            the pais to set
	 */
	public final void setPais(Text pais) {
		this.pais = new Text(pais.toString());
	}

	/**
	 * @return the ciudad
	 */
	public final Text getCiudad() {
		return ciudad;
	}

	/**
	 * @param ciudad
	 *            the ciudad to set
	 */
	public final void setCiudad(Text ciudad) {
		this.ciudad = new Text(ciudad.toString());
	}

	/**
	 * @return the continente
	 */
	public final Text getContinente() {
		return continente;
	}

	/**
	 * @param continente
	 *            the continente to set
	 */
	public final void setContinente(Text continente) {
		this.continente = new Text(continente.toString());
	}
}
