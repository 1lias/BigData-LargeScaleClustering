import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PostScoreViewCountClustering {


  public static void runJob(String[] input, String output) throws Exception {
  int iteration     = 0;               //Iteration Counter
  boolean converged = false;

  while(!converged){

    Job job = Job.getInstance(new Configuration());
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    //Intermediate Path for I/O of the mean values each iteration.
    Path interPath = new Path(output +"/iteration"+iteration+ "/part-r-00000");

    job.setJarByClass(PostScoreViewCountClustering.class);
    job.setMapperClass(PostScoreViewCountClusteringMapper.class);
    job.setReducerClass(PostScoreViewCountClusteringReducer.class);
    job.setMapOutputKeyClass(DoubleDoublePair.class);
    job.setMapOutputValueClass(DoubleDoublePair.class);
    job.setOutputKeyClass(DoubleDoublePair.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(1);

    BufferedReader br ;
    String line;
    ArrayList<DoubleDoublePair> oldCentroids = new ArrayList<DoubleDoublePair>();

    if(iteration == 0){

       conf.set("C1", "0.1\t0.1");      //Initial centroids (3) passed to Mapper via set().
       conf.set("C2", "0.24\t0.5");
       conf.set("C3", "0.52\t0.8");
       oldCentroids.add(new DoubleDoublePair(0.001,0.1));    //Initial centroids (3) added to ArrayList
       oldCentroids.add(new DoubleDoublePair(0.29,0.5));     //in order to compare with new centroids later on.
       oldCentroids.add(new DoubleDoublePair(0.52,0.8));

    }else{
       //Read previous centroids from file.
       br = new BufferedReader(new InputStreamReader(fs.open(interPath)));
       line = br.readLine();

       while (line != null) {

         String[] data = line.split("\t");

         if(data.length == 2){

           DoubleDoublePair centroid
                    = new DoubleDoublePair(Double.parseDouble(data[0]),
                                           Double.parseDouble(data[1]));
           oldCentroids.add(centroid);     //Store old centroids to ArrayList.

         }

         line = br.readLine();

       }

       conf.set("C1", oldCentroids.get(0).toString());  //Sent old centroids via set() to Mapper.
       conf.set("C2", oldCentroids.get(1).toString());
       conf.set("C3", oldCentroids.get(2).toString());

    }

     Path outputPath =  new Path(output +"/iteration"+(iteration+1));
     FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
     FileOutputFormat.setOutputPath(job, outputPath);
     outputPath.getFileSystem(conf).delete(outputPath, true);
     job.waitForCompletion(true);

     //Path of new centroids.
     Path newCentroidsPath = new Path(output +"/iteration"+(iteration+1)+ "/part-r-00000");

     ArrayList<DoubleDoublePair> newCentroids = new ArrayList<DoubleDoublePair>();   //Load new centroids to ArrayList.
     br = new BufferedReader(new InputStreamReader(fs.open(newCentroidsPath)));
     line = br.readLine();

     while (line != null) {

       String[] data = line.split("\t");

       if(data.length == 2){

         DoubleDoublePair centroid
                  = new DoubleDoublePair(Double.parseDouble(data[0]),
                                         Double.parseDouble(data[1]));
         newCentroids.add(centroid);  //Load new centroids to ArrayList.

       }

       line = br.readLine();

     }

     br.close();

       //Comparison of old centroids with new. Converge when the difference is less or equal to 0.1.
      Iterator<DoubleDoublePair> iteratorOldCentroids = oldCentroids.iterator();

      /**********************************************************
       * Compare the x and y axis of the old centroids with the
       * x and y axis of the new centroids.The distance metric
       * used is Euclidean Distance.
       *********************************************************/
      for (DoubleDoublePair newCentroid : newCentroids) {

    	    DoubleDoublePair oldCentroid = iteratorOldCentroids.next();
          double oldCentroidX = oldCentroid.getX().get();
          double oldCentroidY = oldCentroid.getY().get();

          double newCentroidX = newCentroid.getX().get();
          double newCentroidY = newCentroid.getY().get();

          double distance = Math.sqrt(Math.pow(newCentroidX-oldCentroidX,2) +
                                      Math.pow(newCentroidY-oldCentroidY,2));

    	      if (distance <= 0.01) {
    		           converged = true;
    	      } else {
    		           converged = false;
    		           break;
    	      }
     }
      ++iteration;
  }
     clusterDataPoints(input, output, --iteration); //MapReduce job to write centroids including corresponding points.
  }

  /*********************************************************************************
   * clusterDataPoints method re-runs a MapReduce with the final means of the three
   * clusters. It writes on the output file, the centroids with their corresponding
   * data points.
   ********************************************************************************/
  public static void clusterDataPoints(String[] input, String output, int iteration)
                                      throws Exception {

     Job job = Job.getInstance(new Configuration());
     Configuration conf = job.getConfiguration();
     FileSystem fs = FileSystem.get(conf);

     Path interPath = new Path(output +"/iteration"+iteration+ "/part-r-00000");

     job.setJarByClass(PostScoreViewCountClustering.class);
     job.setMapperClass(PostScoreViewCountClusteringMapper.class);
     job.setReducerClass(PostScoreViewCountDataPointsReducer.class);
     job.setMapOutputKeyClass(DoubleDoublePair.class);
     job.setMapOutputValueClass(DoubleDoublePair.class);
     job.setOutputKeyClass(DoubleDoublePair.class);
     job.setOutputValueClass(Text.class);

     BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(interPath)));
     ArrayList<DoubleDoublePair> centroids = new ArrayList<DoubleDoublePair>();
     String line = br.readLine();

     while (line != null) {

       String[] data = line.split("\t");

       if(data.length == 2){

         DoubleDoublePair centroid
                  = new DoubleDoublePair(Double.parseDouble(data[0]),
                                         Double.parseDouble(data[1]));
         centroids.add(centroid);
         line = br.readLine();

      }
     }

     br.close();

     conf.set("C1", centroids.get(0).toString());
     conf.set("C2", centroids.get(1).toString());
     conf.set("C3", centroids.get(2).toString());

     Path outputPath = new Path(output +"/iteration"+iteration+ "withDataPoints");
     FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
     FileOutputFormat.setOutputPath(job, outputPath);
     outputPath.getFileSystem(conf).delete(outputPath, true);
     job.waitForCompletion(true);

  }

  public static void main(String[] args) throws Exception {
    runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
  }
}
