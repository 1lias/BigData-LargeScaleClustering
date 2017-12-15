import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;


public class MinMaxCommentsReducer extends Reducer<NullWritable, DoubleDoublePair, NullWritable, DoubleDoublePair> {

    public void reduce(NullWritable key,
                       Iterable<DoubleDoublePair> data, Context context)
												throws IOException, InterruptedException {

       for(DoubleDoublePair point : data){
         context.write(NullWritable.get(),point);
       }
    }
}
