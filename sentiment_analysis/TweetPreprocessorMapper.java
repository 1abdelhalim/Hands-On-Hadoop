import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TweetPreprocessorMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Set<String> positiveWords;
    private Set<String> negativeWords;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Define some basic positive and negative words for sentiment analysis
        positiveWords = new HashSet<>();
        positiveWords.add("love");
        positiveWords.add("fantastic");
        positiveWords.add("great");
        positiveWords.add("good");
        positiveWords.add("amazing");
        positiveWords.add("wonderful");
        positiveWords.add("thrilled");
        negativeWords = new HashSet<>();
        negativeWords.add("not");
        negativeWords.add("unhappy");
        negativeWords.add("bad");
        negativeWords.add("sad");
        negativeWords.add("disappointed");
        negativeWords.add("down");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line
        String[] tweet = value.toString().split(",", 2);
        String tweetId = tweet[0];
        String tweetText = tweet[1].toLowerCase();

        // Determine sentiment
        String sentiment = "neutral";
        for (String word : tweetText.split(" ")) {
            if (positiveWords.contains(word)) {
                sentiment = "positive";
                break;
            } else if (negativeWords.contains(word)) {
                sentiment = "negative";
                break;
            }
        }

        // Emit the tweetId and the detected sentiment
        context.write(new Text(tweetId), new Text(sentiment));
    }
}

