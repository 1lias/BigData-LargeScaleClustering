import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;


public class MinMaxCommentsReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable key,
                       Iterable<IntWritable> data, Context context)
												throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable occur : data){
          sum += occur.get();
        }
        System.out.println("REDUCER: " + sum);
       context.write(key,new IntWritable(sum));
    }
}
