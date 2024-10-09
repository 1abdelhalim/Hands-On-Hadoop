import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GeospatialReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        StringBuilder sb = new StringBuilder();

        // Count the number of coordinates per region and accumulate them in the result
        for (Text val : values) {
            count++;
            sb.append(val.toString()).append(" ");
        }

        result.set("Number of locations: " + count + ", Coordinates: " + sb.toString().trim());
        context.write(key, result);
    }
}
