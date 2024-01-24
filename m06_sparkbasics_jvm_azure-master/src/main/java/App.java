import models.Weather;
import models.Hotel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.SchemaUtils;

public class App {

    // Declare constants for environment variable names
    private static final String OAUTH2_CLIENT_ID = "OAUTH2_CLIENT_ID";
    private static final String OAUTH2_CLIENT_SECRET = "OAUTH2_CLIENT_SECRET";
    private static final String OAUTH2_CLIENT_ENDPOINT = "OAUTH2_CLIENT_ENDPOINT";
    private static final String SESSION_MASTER = "SESSION_MASTER";
    private static final String INPUT_HOTELS_PATH = ;


    public static void main(String[] args) {


        String oAuthClientId = System.getenv(OAUTH2_CLIENT_ID);
        String oAuthClientSecret = System.getenv(OAUTH2_CLIENT_SECRET);
        String oAuthClientEndpoint = System.getenv(OAUTH2_CLIENT_ENDPOINT);

        SparkSession session = SparkSession.builder()
                .appName("Spark Basics Homework") // Sets the application name
                .master(System.getenv(SESSION_MASTER)) // Sets the Spark master URL (can be "local", "yarn", etc.)
                .config("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth") // Configures ADLS authentication type
                .config("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azuurebfs.oauth") // Configures OAuth provider type
                .config("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", oAuthClientId) // Configures OAuth client ID
                .config("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", oAuthClientSecret) // Configures OAuth client secret
                .config("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", oAuthClientEndpoint)   // Configures OAuth client endpoint
                .getOrCreate();

        Dataset<Hotel> hotels = session.read()
                .option("header", "true")
                .schema(SchemaUtils.getHotelSchema())
                .csv(System.getenv(INPUT_HOTELS_PATH))
                .map(new ParseHotelFunction(), Encoders.bean(Hotel.class))
                .map(new RequestCoordinatesFunction(), Encoders.bean(Hotel.class))
                .map(new GenerateGeoHashForHotel(), Encoders.bean(Hotel.class))
                .as("hotels");

        Dataset<Weather> weather = session.read()
                .schema(SchemaUtils.getWeatherSchema())
                .parquet(System.getenv(INPUT_WEATHER_PATH))
                .map(new ParseWeatherFunction(), Encoders.bean(Weather.class))
                .map(new GenerateGeohashForWeather(), Encoders.bean(Weather.class))
                .as("weather");

        Dataset<Row> joined = hotels //performing left join between hotels and weathers
                .join(weather, weather.col("geo_hash").equalTo(hotels.col("geoHash")), "left");

        joined.write().parquet(OutputPath.getOutPutPath(System.getenv(OUTPUT_BASE_PATH), session.sparkContext().applicationId())); //saves the result of the left join operation  into Parquet format

        session.stop(); //shut down the Spark application.

    }
}