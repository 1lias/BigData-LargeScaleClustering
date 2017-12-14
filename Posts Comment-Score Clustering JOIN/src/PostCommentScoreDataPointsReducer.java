import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;


public class PostCommentScoreDataPointsReducer extends Reducer<DoubleDoublePair, DoubleDoublePair, DoubleDoublePair, Text> {

    public void reduce(DoubleDoublePair centroid,
                       Iterable<DoubleDoublePair> data, Context context)
												throws IOException, InterruptedException {
       String points = "<- Centroid + \n";
       for(DoubleDoublePair dataPoint: data){
         points += dataPoint.toString() + "\n";
       }

       context.write(centroid, new Text(points));

    }
}
