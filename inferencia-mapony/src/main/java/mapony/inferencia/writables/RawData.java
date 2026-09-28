// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.writables;

import java.io.Serializable;

import mapony.inferencia.geoHash.bean.GeoHashBean;
import mapony.inferencia.util.cte.InferenciaCte;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated from WritableComparable to Serializable for Apache Spark compatibility.
 * Text fields replaced with String so RawData instances can be serialized across
 * Spark executors without requiring Hadoop's Writable serialization framework.
 */
public class RawData implements Comparable<RawData>, Serializable {

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

    public RawData() { set(); }
    public RawData(final String[] data, final GeoHashBean ghb) { set(data, ghb); }
    public RawData(final String[] data, final String geoHash) { set(data, geoHash); }
    public RawData(final String[] data, final String geoHash, final String city) {
        set(data[0], data[3], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[14], geoHash, city);
    }
    public RawData(final RawData rd) { set(rd); }
    public RawData(final RawData rd, final String newGeoHash) { set(rd, newGeoHash); }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
        result = prime * result + ((longitude == null) ? 0 : longitude.hashCode());
        result = prime * result + ((latitude == null) ? 0 : latitude.hashCode());
        return result;
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

    private void set(final String identifier, final String dateTaken, final String captureDevice,
                     final String title, final String description, final String userTags,
                     final String machineTags, final String longitude, final String latitude,
                     final String downloadUrl, final String geoHash, final String ciudad) {
        this.identifier = identifier;
        this.dateTaken = dateTaken;
        this.captureDevice = captureDevice;
        this.title = title;
        this.description = description;
        this.userTags = userTags;
        this.machineTags = machineTags;
        this.longitude = longitude;
        this.latitude = latitude;
        this.downloadUrl = downloadUrl;
        this.geoHash = geoHash;
        this.ciudad = ciudad;
    }

    private void set(final RawData rd) {
        set(rd.identifier, rd.dateTaken, rd.captureDevice, rd.title, rd.description,
            rd.userTags, rd.machineTags, rd.longitude, rd.latitude, rd.downloadUrl,
            rd.geoHash, rd.ciudad);
    }

    private void set(final RawData rd, final String newGeoHash) {
        set(rd.identifier, rd.dateTaken, rd.captureDevice, rd.title, rd.description,
            rd.userTags, rd.machineTags, rd.longitude, rd.latitude, rd.downloadUrl,
            newGeoHash, rd.ciudad);
    }

    private void set(final String[] data, final GeoHashBean ghb) {
        set(data[0], data[3], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[14],
            ghb.getGeoHash(), ghb.getCity());
    }

    private void set(final String[] data, final String geoHash) {
        set(data[0], data[3], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[14],
            geoHash, InferenciaCte.EMPTY_STRING);
    }

    public int compareTo(RawData o) { return identifier.compareTo(o.identifier); }

    public boolean equals(Object o) {
        if (!(o instanceof RawData)) return false;
        return identifier.equals(((RawData) o).identifier);
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
