import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class DataPreprocessingMapper extends Mapper<Object, Text, NullWritable, DoubleDoublePair> {
  private Map<String,String> featuresMap;   //Map of post extracted from XML
  private DoubleDoublePair xAxis_yAxis;     //Pair oject with Scor eand ViewCount of post.

  @Override
  public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

    int centroidIndex = 0;
    featuresMap = transformXmlToMap(data.toString());

    try{

      xAxis_yAxis = new DoubleDoublePair(Double.parseDouble(featuresMap.get("Score")),       //Retrieve the Score for the post from Map.
                                         Double.parseDouble(featuresMap.get("ViewCount")));  //Retrieve the ViewCount for the post from Map.
      context.write(NullWritable.get(),xAxis_yAxis);                                         //Emit NullWritable and the Pair of values.

    }catch(NullPointerException e){}

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
