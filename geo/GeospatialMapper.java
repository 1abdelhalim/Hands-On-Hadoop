import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GeospatialMapper extends Mapper<Object, Text, Text, Text> {

    private Text region = new Text();
    private Text coordinates = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (!line.isEmpty()) {
            String[] parts = line.split(",");
            if (parts.length == 2) {
                try {
                    double latitude = Double.parseDouble(parts[0]);
                    double longitude = Double.parseDouble(parts[1]);

                    // Determine region based on latitude and longitude
                    String regionName = determineRegion(latitude, longitude);
                    region.set(regionName);
                    coordinates.set(parts[0] + "," + parts[1]);

                    // Emit region and coordinates as key-value pair
                    context.write(region, coordinates);
                } catch (NumberFormatException e) {
                    // Skip invalid latitude/longitude values
                }
            }
        }
    }

    // region determination logic
    private String determineRegion(double lat, double lon) {
        if (lat > 40.0 && lon < -70.0) return "Northeast";
        if (lat < 40.0 && lon < -100.0) return "Midwest";
        if (lat > 35.0 && lon > -90.0) return "South";
        if (lat > 40.0 && lon < -120.0) return "Northwest";
        if (lat < 35.0 && lon > -110.0) return "Southwest";
        return "Unknown";
    }
}
