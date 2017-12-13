import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.io.*;
import java.nio.channels.FileChannel;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StackoverflowClustering {



   public static void runJob(String[] input, String output) throws Exception {
     int iteration = 0;
     boolean clustered = false;

     while(!clustered){
       Job job = Job.getInstance(new Configuration());
       Configuration conf = job.getConfiguration();
       FileSystem fs = FileSystem.get(conf);
      
       Path interPath = new Path(output +"/iteration"+iteration+ "/part-r-00000");



       job.setJarByClass(StackoverflowClustering.class);
       job.setMapperClass(StackoverflowClusteringMapper.class);
       job.setReducerClass(StackoverflowClusteringReducer.class);
       job.setMapOutputKeyClass(DoubleWritable.class);
       job.setMapOutputValueClass(DoubleWritable.class);
       job.setOutputKeyClass(DoubleWritable.class);
       job.setOutputValueClass(NullWritable.class);
       job.setNumReduceTasks(1);

       BufferedReader br ;
       String line;
       List<Double> oldCentroids = new ArrayList<Double>();

       if(iteration == 0){
         conf.set("C1", "15");
         conf.set("C2", "25");
         conf.set("C3", "45");
         oldCentroids.add(15.0);
         oldCentroids.add(25.0);
         oldCentroids.add(45.0);
       }else{
         br = new BufferedReader(new InputStreamReader(fs.open(interPath)));
         line = br.readLine();
         while (line != null) {
           double centroid = Double.parseDouble(line);
           System.out.println(line);
           oldCentroids.add(centroid);
           line = br.readLine();
         }
         br.close();

         conf.set("C1", oldCentroids.get(0).toString());
         conf.set("C2", oldCentroids.get(1).toString());
         conf.set("C3", oldCentroids.get(2).toString());
       }

         Path outputPath = new Path(output);
         FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
         FileOutputFormat.setOutputPath(job, outputPath);
         outputPath.getFileSystem(conf).delete(outputPath, true);
         job.waitForCompletion(true);

         //Load new centroids to ArrayList
         br = new BufferedReader(new InputStreamReader(fs.open(interPath)));
         List<Double> newCentroids = new ArrayList<Double>();
         line = br.readLine();
         while (line != null) {
           double centroid = Double.parseDouble(line);
           newCentroids.add(centroid);
           line = br.readLine();
         }
         br.close();

         //Compare old with new centroids and if at least one of them has more than 0.1 differemce then repeat MapReduce
         Iterator<Double> iteratorOldCentroids = oldCentroids.iterator();
 			  for (double newCentroid : newCentroids) {
 				      double oldCentroid = iteratorOldCentroids.next();
 				      if (Math.abs(oldCentroid - newCentroid) <= 0.1) {
 					           clustered = true;
 				      } else {
 					           clustered = false;
 					           break;
 				      }
         }
          ++iteration;
       }

    }


    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
