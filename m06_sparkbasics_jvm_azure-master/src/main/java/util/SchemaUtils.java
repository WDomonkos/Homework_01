package util;

import org.apache.spark.sql.types.StructType;

public class SchemaUtils {

    private static final String STRING_TYPE = "string";

    private static final String DOUBLE_TYPE = "double";

    public static StructType getHotelSchema() {
        return new StructType()
                .add("hotelId", STRING_TYPE)
                .add("hotelName", STRING_TYPE)
                .add("city", STRING_TYPE)
                .add("country", STRING_TYPE);
                // Add other fields as needed

    }

    public static StructType getWeatherSchema() {
        return new StructType()
                .add("long", DOUBLE_TYPE)
                .add("lat", DOUBLE_TYPE)
                .add("avg_tmpr_f", DOUBLE_TYPE)
                .add("wthr_date", STRING_TYPE);
                // Add other fields as needed
        
    }
}