package models;

import org.apache.spark.sql.Row;
import ch.hsr.geohash.GeoHash;
import java.io.Serializable;

public class Hotel implements Serializable {

    private Double latitude;
    private Double longitude;
    private String geoHash;

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public String getGeoHash() {
        return geoHash;
    }

    public void setGeoHash(String geoHash) {
        this.geoHash = geoHash;
    }

    public Void ParseHotelFunction(Row row) {
        if (!row.isNullAt(row.fieldIndex("latitude"))) {
            this.latitude = row.getDouble(row.fieldIndex("latitude"));
        }

        if (!row.isNullAt(row.fieldIndex("longitude"))) {
            this.longitude = row.getDouble(row.fieldIndex("longitude"));
        }

    }

    public void generateGeoHash() {
        if (latitude != null && longitude != null) {
            Hotel GeoHashUtils = null;
            this.geoHash = GeoHashUtils.getGeoHash(latitude, longitude);
        }
    }

    public String getGeoHash(Double latitude, Double longitude) {
        return null;
    }


}