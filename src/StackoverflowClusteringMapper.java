import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StackoverflowClusteringMapper extends Mapper<Object, Text, DoubleWritable, DoubleWritable> {
  private Map<String,String> userMap;
  private ArrayList<DoubleWritable> centroids;
  private DoubleWritable age;

  @Override
  public void setup(Context context)throws IOException,
                                       InterruptedException{


        // We know there is only one cache file, so we only retrieve that URI
        URI fileUri = context.getCacheFiles()[0];

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(fileUri));

        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line = null;
        try {
            // we discard the header row
            br.readLine();

            while ((line = br.readLine()) != null){
              String[] data = line.split(" ");
              if(data.length > 0){
                centroids.add(new DoubleWritable(Double.parseDouble(data[0])));
              }
            }


            br.close();
        } catch (IOException e1) {
        }

        super.setup(context);
  }

  @Override
	public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {
    String[] lines = data.toString().split("/t");

    if(lines.length == 0){
       int centroidIndex = 0;
       userMap = transformXmlToMap(data.toString());
       age = new DoubleWritable(Double.parseDouble(userMap.get("Age")));
       double minDist = centroids.get(0).get() - age.get();


      for(int i = 1; i < centroids.size(); i++){
        double next = centroids.get(i).get() - age.get();
        if(Math.abs(next) < Math.abs(minDist)){
          minDist = next;
          centroidIndex = i;
        }
      }
      context.write(centroids.get(centroidIndex),age);
    }else{
      age = new DoubleWritable(Double.parseDouble(lines[1]));
      double minDist = centroids.get(0).get() -  age.get();
      int centroidIndex = 0;

      for(int i = 1; i < centroids.size(); i++){
        double next = centroids.get(i).get() - age.get();
        if(Math.abs(next) < Math.abs(minDist)){
          minDist = next;
          centroidIndex = i;
        }
      }
      context.write(centroids.get(centroidIndex),age);
    }
	}


  /*******************************************************************
   * This helper function parses the stackoverflow into a Map for us.
   * Taken from https://goo.gl/LLd2Zn
   ******************************************************************/
  public static Map<String, String> transformXmlToMap(String xml) {
  	Map<String, String> map = new HashMap<String, String>();
  	try {
  		String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
  				.split("\"");

  		for (int i = 0; i < tokens.length - 1; i += 2) {
  			String key = tokens[i].trim();
  			String val = tokens[i + 1];

  			map.put(key.substring(0, key.length() - 1), val);
  		}
  	} catch (StringIndexOutOfBoundsException e) {
  		System.err.println(xml);
  	}

  	return map;
  }
}
