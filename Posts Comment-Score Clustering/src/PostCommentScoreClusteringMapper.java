import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserAgeClusteringMapper extends Mapper<Object, Text, DoubleWritable, DoubleWritable> {
  private Map<String,String> userMap;
  private ArrayList<DoubleWritable> centroids = new ArrayList<DoubleWritable>();
  private DoubleWritable age;



  @Override
	public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

       int centroidIndex = 0;
       userMap = transformXmlToMap(data.toString());
       try{
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
    }catch(NullPointerException e){}

	}

  @Override
  protected void setup(Context context)throws IOException,
                                       InterruptedException{

     Configuration conf = context.getConfiguration();
     centroids.add(new DoubleWritable(Double.parseDouble(conf.get("C1"))));
     centroids.add(new DoubleWritable(Double.parseDouble(conf.get("C2"))));
     centroids.add(new DoubleWritable(Double.parseDouble(conf.get("C3"))));
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
