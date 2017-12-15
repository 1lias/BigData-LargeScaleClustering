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

public class PostCommentScoreDataPointsMapper extends Mapper<Object, Text, DoubleDoublePair, DoubleDoublePair> {
  private Map<String,String> userMap;
  private ArrayList<DoubleDoublePair> centroids = new ArrayList<DoubleDoublePair>();
  private DoubleDoublePair commentCount_Score;



  @Override
	public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

       int centroidIndex = 0;
       userMap = transformXmlToMap(data.toString());
       try{

       commentCount_Score = new DoubleDoublePair(Double.parseDouble(userMap.get("Score")),
                                                 Double.parseDouble(userMap.get("ViewCount")));

       double centroidX = centroids.get(0).getX().get();
       double centroidY = centroids.get(0).getY().get();

       double dataPointX = ((commentCount_Score.getX().get()) - -166.0)/6683;
       double dataPointY = ((commentCount_Score.getY().get()) - 0)/1677209;

       double minDist = Math.sqrt(Math.pow(dataPointX-centroidX,2)+
                                  Math.pow(dataPointY-centroidY,2));


      for(int i = 1; i < centroids.size(); i++){

        centroidX = centroids.get(i).getX().get();
        centroidY = centroids.get(i).getY().get();

        double next = Math.sqrt(Math.pow(dataPointX-centroidX,2)+
                                Math.pow(dataPointY-centroidY,2));

        if(Math.abs(next) < Math.abs(minDist)){
          minDist = next;
          centroidIndex = i;
        }
      }
      context.write(centroids.get(centroidIndex),commentCount_Score);
    }catch(NullPointerException e){}

	}

  @Override
  protected void setup(Context context)throws IOException,
                                       InterruptedException{

     Configuration conf = context.getConfiguration();

     String[] centroidValues = conf.get("C1").split("\t");
     centroids.add(new DoubleDoublePair(Double.parseDouble(centroidValues[0]),
                                        Double.parseDouble(centroidValues[1])));

     centroidValues = conf.get("C2").split("\t");
     centroids.add(new DoubleDoublePair(Double.parseDouble(centroidValues[0]),
                                        Double.parseDouble(centroidValues[1])));

     centroidValues = conf.get("C3").split("\t");
     centroids.add(new DoubleDoublePair(Double.parseDouble(centroidValues[0]),
                                        Double.parseDouble(centroidValues[1])));
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
