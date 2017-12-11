import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.nio.channels.FileChannel;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StackoverflowClustering {



   public static void runJob(String[] input, String output) throws Exception {
     int iteration = 0;
     boolean clustered = false;

     Path oldCentroidsPath=new Path(output+"/previousCentroids.txt");
     Path newCentroidsPath = new Path(output+"/currentCentroids.txt");




     while(!clustered){
       Job job = Job.getInstance(new Configuration());
       Configuration conf = job.getConfiguration();

        if(iteration == 0){
             System.out.println(System.getProperty("user.dir"));
          Writer writer = new BufferedWriter(new OutputStreamWriter(
              new FileOutputStream(newCentroidsPath.toString())));


          writer.write("15 30 50");
        }else{
          FileChannel src = new FileInputStream(new File(newCentroidsPath.toString())).getChannel();
          FileChannel dest = new FileOutputStream(new File(oldCentroidsPath.toString())).getChannel();
          dest.transferFrom(src, 0, src.size());
        }
        job.addCacheFile(newCentroidsPath.toUri());



        job.addCacheFile(newCentroidsPath.toUri());
        job.setJarByClass(StackoverflowClustering.class);

        job.setReducerClass(StackoverflowClusteringReducer.class);
        job.setMapperClass(StackoverflowClusteringMapper.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
    		job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(0);



        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, newCentroidsPath);
        newCentroidsPath.getFileSystem(conf).delete(newCentroidsPath, true);
        job.waitForCompletion(true);


        //Load old centroids to ArrayList
        BufferedReader br = new BufferedReader(new FileReader(oldCentroidsPath.toString()));
        List<Double> oldCentroids = new ArrayList<Double>();
        String line = br.readLine();
        if (line != null) {
          String[] values = line.split(" ");
          double centroid = Double.parseDouble(values[0]);
          oldCentroids.add(centroid);
          line = br.readLine();
        }
        br.close();

        //Load new centroids to ArrayList
        br = new BufferedReader(new FileReader(newCentroidsPath.toString()));
        List<Double> newCentroids = new ArrayList<Double>();
        line = br.readLine();
        if (line != null) {
          String[] values = line.split(" ");
          double centroid = Double.parseDouble(values[0]);
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

        iteration++;
      }
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
