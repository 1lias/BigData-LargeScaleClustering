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

public class MinMaxComments {



   public static void runJob(String[] input, String output) throws Exception {
      Job job = Job.getInstance(new Configuration());
      Configuration conf = job.getConfiguration();

      job.setJarByClass(MinMaxComments.class);
      job.setMapperClass(MinMaxCommentsMapper.class);
      job.setReducerClass(MinMaxCommentsReducer.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(DoubleDoublePair.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(DoubleDoublePair.class);

      Path outputPath = new Path(output);
      FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
      FileOutputFormat.setOutputPath(job, outputPath);
      outputPath.getFileSystem(conf).delete(outputPath, true);
      job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
