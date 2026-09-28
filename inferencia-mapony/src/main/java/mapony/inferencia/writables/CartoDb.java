// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.writables;

import java.io.Serializable;

import mapony.inferencia.util.Utilities;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.validation.SimpleValidation;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated from WritableComparable to Serializable for Apache Spark compatibility.
 * Text fields replaced with String — see RawData for the same rationale.
 */
public class CartoDb implements Comparable<CartoDb>, Serializable {

    private static final long serialVersionUID = 1L;

    private String identifier;
    private String dateTaken;
    private String captureDevice;
    private String title;
    private String description;
    private String userTags;
    private String machineTags;
    private String longitude;
    private String latitude;
    private String downloadUrl;
    private String geoHash;
    private String ciudad;

    public CartoDb() { set(); }

    public CartoDb(final RawData rd) { set(rd); }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
        result = prime * result + ((longitude == null) ? 0 : longitude.hashCode());
        result = prime * result + ((latitude == null) ? 0 : latitude.hashCode());
        return result;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(identifier).append(InferenciaCte.COMMA);
        sb.append(dateTaken).append(InferenciaCte.COMMA);
        // replacePlusFromString replaces the '+' URL encoding that Flickr uses for spaces
        if (SimpleValidation.isTrimExpectedEqualsEmpty(captureDevice)) {
            sb.append(InferenciaCte.SPACE).append(InferenciaCte.COMMA);
        } else {
            sb.append(Utilities.replacePlusFromString(captureDevice)).append(InferenciaCte.COMMA);
        }
        sb.append(ciudad).append(InferenciaCte.COMMA);
        sb.append(longitude).append(InferenciaCte.COMMA);
        sb.append(latitude);
        return sb.toString();
    }

    private void set() {
        this.identifier = InferenciaCte.EMPTY_STRING;
        this.dateTaken = InferenciaCte.EMPTY_STRING;
        this.captureDevice = InferenciaCte.EMPTY_STRING;
        this.title = InferenciaCte.EMPTY_STRING;
        this.description = InferenciaCte.EMPTY_STRING;
        this.userTags = InferenciaCte.EMPTY_STRING;
        this.machineTags = InferenciaCte.EMPTY_STRING;
        this.longitude = InferenciaCte.EMPTY_STRING;
        this.latitude = InferenciaCte.EMPTY_STRING;
        this.downloadUrl = InferenciaCte.EMPTY_STRING;
        this.geoHash = InferenciaCte.EMPTY_STRING;
        this.ciudad = InferenciaCte.EMPTY_STRING;
    }

    private void set(final RawData rd) {
        this.identifier = rd.getIdentifier();
        this.dateTaken = rd.getDateTaken();
        this.captureDevice = rd.getCaptureDevice();
        this.title = rd.getTitle();
        this.description = rd.getDescription();
        this.userTags = rd.getUserTags();
        this.machineTags = rd.getMachineTags();
        this.longitude = rd.getLongitude();
        this.latitude = rd.getLatitude();
        this.downloadUrl = rd.getDownloadUrl();
        this.geoHash = rd.getGeoHash();
        this.ciudad = rd.getCiudad();
    }

    public int compareTo(CartoDb o) { return identifier.compareTo(o.identifier); }

    public boolean equals(Object o) {
        if (!(o instanceof CartoDb)) return false;
        return identifier.equals(((CartoDb) o).identifier);
    }

    public final String getIdentifier() { return identifier; }
    public final void setIdentifier(String identifier) { this.identifier = identifier; }
    public final String getDateTaken() { return dateTaken; }
    public final void setDateTaken(String dateTaken) { this.dateTaken = dateTaken; }
    public final String getCaptureDevice() { return captureDevice; }
    public final void setCaptureDevice(String captureDevice) { this.captureDevice = captureDevice; }
    public final String getTitle() { return title; }
    public final void setTitle(String title) { this.title = title; }
    public final String getDescription() { return description; }
    public final void setDescription(String description) { this.description = description; }
    public final String getUserTags() { return userTags; }
    public final void setUserTags(String userTags) { this.userTags = userTags; }
    public final String getMachineTags() { return machineTags; }
    public final void setMachineTags(String machineTags) { this.machineTags = machineTags; }
    public final String getLongitude() { return longitude; }
    public final void setLongitude(String longitude) { this.longitude = longitude; }
    public final String getLatitude() { return latitude; }
    public final void setLatitude(String latitude) { this.latitude = latitude; }
    public final String getDownloadUrl() { return downloadUrl; }
    public final void setDownloadUrl(String downloadUrl) { this.downloadUrl = downloadUrl; }
    public final String getGeoHash() { return geoHash; }
    public final void setGeoHash(String geoHash) { this.geoHash = geoHash; }
    public final String getCiudad() { return ciudad; }
    public final void setCiudad(String ciudad) { this.ciudad = ciudad; }
}
