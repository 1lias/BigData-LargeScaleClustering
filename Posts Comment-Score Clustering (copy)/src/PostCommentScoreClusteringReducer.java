import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;


public class PostCommentScoreClusteringReducer extends Reducer<DoubleDoublePair, DoubleDoublePair, DoubleDoublePair, NullWritable> {

    public void reduce(DoubleDoublePair centroid,
                       Iterable<DoubleDoublePair> data, Context context)
												throws IOException, InterruptedException {
       double sumX = 0;
       double sumY = 0;
       int numEl = 0;

       for(DoubleDoublePair commentCount_Score : data){
         sumX += commentCount_Score.getX().get();
         sumY += commentCount_Score.getY().get();
         numEl++;
       }

       double newCentroidX = Math.round((sumX / numEl)*100.0)/100.0;
       double newCentroidY = Math.round((sumY / numEl)*100.0)/100.0;

       DoubleDoublePair newCentroid = new  DoubleDoublePair(newCentroidX,newCentroidY);

       context.write(newCentroid, NullWritable.get());
    }
}
