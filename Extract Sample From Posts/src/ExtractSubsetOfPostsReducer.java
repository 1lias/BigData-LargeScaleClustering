import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;



public class ExtractSubsetOfPostsReducer extends Reducer<NullWritable, DoubleDoublePair, NullWritable, DoubleDoublePair> {

    public void reduce(NullWritable key,
                       Iterable<DoubleDoublePair> data, Context context)
												throws IOException, InterruptedException {

       for(DoubleDoublePair point : data){
         context.write(NullWritable.get(),point);     //Write the Score and ViewCount for each post in the Iterable.
       }

    }
}
