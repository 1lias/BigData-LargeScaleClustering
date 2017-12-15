import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;


public class JoinUserBadgesReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable key,
                       Iterable<IntWritable> data, Context context)
												throws IOException, InterruptedException {

        int sum = 0;     //Aggregation of Badges for the particular user age.

        for(IntWritable occur : data){
          sum += occur.get();
        }

       //Emit the User Age as key and the corresponding number of badges as value.
       context.write(key,new IntWritable(sum));

    }
}
