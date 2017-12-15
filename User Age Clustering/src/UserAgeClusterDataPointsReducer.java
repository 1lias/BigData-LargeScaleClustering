import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class UserAgeClusterDataPointsReducer extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text> {

    public void reduce(DoubleWritable centroid, Iterable<DoubleWritable> data, Context context)
												                                     throws IOException, InterruptedException {

       String points = "<- Centroid + \n";                    //Concatenate all data points in one String.

       for(DoubleWritable age : data){

         points += age.toString() + "\n";

       }

       context.write(centroid, new Text(points));             //Emit the centroid with its corresponding data points.
    }
}
