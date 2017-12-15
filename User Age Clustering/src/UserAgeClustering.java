import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserAgeClustering {

  public static void runJob(String[] input, String output) throws Exception {
   int iteration = 0;               //Iteration Counter
   boolean clustered = false;

   while(!clustered){
     Job job = Job.getInstance(new Configuration());
     Configuration conf = job.getConfiguration();
     FileSystem fs = FileSystem.get(conf);
     
     //Intermediate Path for I/O of the mean values each iteration.
     Path interPath = new Path(output +"/iteration"+iteration+ "/part-r-00000");

     BufferedReader br ;
     String line;
     ArrayList<Double> oldCentroids = new ArrayList<Double>();

     job.setJarByClass(UserAgeClustering.class);
     job.setMapperClass(UserAgeClusteringMapper.class);
     job.setReducerClass(UserAgeClusteringReducer.class);
     job.setMapOutputKeyClass(DoubleWritable.class);
     job.setMapOutputValueClass(DoubleWritable.class);
     job.setOutputKeyClass(DoubleWritable.class);
     job.setOutputValueClass(NullWritable.class);
     job.setNumReduceTasks(1);

     if(iteration == 0){

       conf.set("C1", "15");      //Initial centroids (3) passed to Mapper via set().
       conf.set("C2", "25");
       conf.set("C3", "45");
       oldCentroids.add(15.0);    //Initial centroids (3) added to ArrayList
       oldCentroids.add(25.0);    //in order to compare with new centroids later on.
       oldCentroids.add(45.0);

     }else{
       //Read previous centroids from file.
       br = new BufferedReader(new InputStreamReader(fs.open(interPath)));
       line = br.readLine();

       while (line != null) {

         double centroid = Double.parseDouble(line);
         oldCentroids.add(centroid);  //Store old centroids to ArrayList.
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

       ArrayList<Double> newCentroids = new ArrayList<Double>();   //Load new centroids to ArrayList.
       br = new BufferedReader(new InputStreamReader(fs.open(newCentroidsPath)));
       line = br.readLine();

       while (line != null) {

         double centroid = Double.parseDouble(line);
         newCentroids.add(centroid);  //Load new centroids to ArrayList.
         line = br.readLine();

       }

       br.close();

       //Comparison of old centroids with new. Converge when the difference is less or equal to 0.1.
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

       job.setJarByClass(UserAgeClustering.class);
       job.setMapperClass(UserAgeClusterDataPointsMapper.class);
       job.setReducerClass(UserAgeClusterDataPointsReducer.class);
       job.setMapOutputKeyClass(DoubleWritable.class);
       job.setMapOutputValueClass(DoubleWritable.class);
       job.setOutputKeyClass(DoubleWritable.class);
       job.setOutputValueClass(Text.class);
       job.setNumReduceTasks(1);

       BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(interPath)));
       ArrayList<Double> centroids = new ArrayList<Double>();
       String line = br.readLine();

       while (line != null) {

         double centroid = Double.parseDouble(line);
         centroids.add(centroid);
         line = br.readLine();

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
