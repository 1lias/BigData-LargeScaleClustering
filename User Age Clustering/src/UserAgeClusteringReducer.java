import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;



public class UserAgeClusteringReducer extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, NullWritable> {

    public void reduce(DoubleWritable centroid,Iterable<DoubleWritable> data, Context context)
												                                    throws IOException, InterruptedException {

       double sum = 0;                    //Sum of all Ages for the particular cluster.
       int numEl = 0;                     //Number of elements in the particular cluster.

       for(DoubleWritable age : data){    //Add all the elements and keep a count of them.

         sum += age.get();
         numEl++;

       }

       double newCentroid = Math.round((sum/ numEl * 100.0)) / 100.0;           //Calculate the new Centroid.

       context.write(new DoubleWritable(newCentroid), NullWritable.get());      //Emit the new Centroid with NullWritable as value.

    }
}
