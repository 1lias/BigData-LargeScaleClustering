import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Hashtable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class UserAgeClusteringMapper extends Mapper<Object, Text, DoubleWritable, DoubleWritable> {
  private ArrayList<DoubleWritable> centroids = new ArrayList<DoubleWritable>();
  private Map<String,String> userMap;
  private DoubleWritable age;

  @Override
	public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

       int centroidIndex = 0;
       userMap = transformXmlToMap(data.toString());  //Transform the User entry to Map.

      try{

        age = new DoubleWritable(Double.parseDouble(userMap.get("Age"))); //Retrieve the age of the User entry.
        double minDist = centroids.get(0).get() - age.get();              //Set the minimum distance as the Centroid 1 - Age.

        //Loop through the three centroids.
        for(int i = 1; i < centroids.size(); i++){
        //Calculate the distance of the centroid and the Age.
        double next = centroids.get(i).get() - age.get();
        //Take the smallest distance of all centroids.
        if(Math.abs(next) < Math.abs(minDist)){
          minDist = next;
          centroidIndex = i;
        }

      }
      //Emit the centroid and the Age data point.
      context.write(centroids.get(centroidIndex),age);

    }catch(NullPointerException e){}

	}

  /*******************************************************************
   * Setup method called at the initialisation of the Mapper.
   * It loads the centroids passed from the job configuration class.
   ******************************************************************/
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
