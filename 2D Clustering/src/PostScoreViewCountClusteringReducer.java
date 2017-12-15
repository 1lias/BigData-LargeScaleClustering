import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PostScoreViewCountClusteringReducer extends Reducer<DoubleDoublePair, DoubleDoublePair, DoubleDoublePair, NullWritable> {

    public void reduce(DoubleDoublePair centroid,
                       Iterable<DoubleDoublePair> data, Context context)
												throws IOException, InterruptedException {
       double sumX = 0;   //Sum of X axis of all Posts for the particular cluster.
       double sumY = 0;   //Sum of Y axis or all Posts for the particular cluster.
       int numEl = 0;     //Total number of elements in the cluster.

       for(DoubleDoublePair commentCount_Score : data){
         sumX += commentCount_Score.getX().get();
         sumY += commentCount_Score.getY().get();
         numEl++;
       }

       double newCentroidX = (sumX / numEl);    //Calculate the new mean for the X axis.
       double newCentroidY = (sumY / numEl);    //Calculate the new mean for the Y axis.

       DoubleDoublePair newCentroid = new  DoubleDoublePair(newCentroidX,newCentroidY);
       //Emit the new centroid mean.
       context.write(newCentroid, NullWritable.get());
    }
}
