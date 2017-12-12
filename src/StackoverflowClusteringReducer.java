import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;


public class StackoverflowClusteringReducer extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text> {

    public void reduce(DoubleWritable centroid,
                       Iterable<DoubleWritable> data, Context context)
												throws IOException, InterruptedException {
       double sum = 0;
       int numEl = 0;

       for(DoubleWritable age : data){
         sum += age.get();
         numEl++;
       }

       double newCentroid = Math.round(sum / numEl);
       context.write(new DoubleWritable(newCentroid), NullWritable.get());
    }
}