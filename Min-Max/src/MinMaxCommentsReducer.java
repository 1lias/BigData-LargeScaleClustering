import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;


public class MinMaxCommentsReducer extends Reducer<NullWritable, DoubleDoublePair, NullWritable, Text> {

    public void reduce(NullWritable key,
                       Iterable<DoubleDoublePair> data, Context context)
												throws IOException, InterruptedException {
       double minX = Double.MAX_VALUE;
       double maxX = Double.MIN_VALUE;
       double minY = Double.MAX_VALUE;
       double maxY = Double.MIN_VALUE;

       for(DoubleDoublePair bodyScore : data){

         double tempX = bodyScore.getX().get();
         double tempY = bodyScore.getY().get();

         if(tempX < minX){
           minX = tempX;
         }else if(tempX > maxX){
           maxX = tempX;
         }

         if(tempY < minY){
           minY = tempY;
         }else if(tempY > maxY){
           maxY = tempY;
         }
       }

       String out = "Min X: " + minX + "\tMax X: " + maxX + "\n" +
                    "Min Y: " + minY + "\tMax Y: " + maxY;
       Text output = new Text(out);

       context.write(NullWritable.get(),output);
    }
}
