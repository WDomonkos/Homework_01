package functions;

import models.Hotel;
import org.apache.spark.api.java.function.MapFunction;


public class GenerateGeoHashForHotel implements MapFunction<Hotel, Hotel> {

    @Override
    public Hotel call(Hotel hotel) {
        Hotel GeoHashUtils = null;
        hotel.setGeoHash(GeoHashUtils.getGeoHash(hotel.getLatitude(), hotel.getLongitude()));
        return hotel;
    }
}
