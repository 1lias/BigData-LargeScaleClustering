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

       int bodyLength = userMap.get("Body").length();
       commentCount_Score = new DoubleDoublePair((double)bodyLength,
                                                 Double.parseDouble(userMap.get("Score")));
       double centroidX = Math.round(centroids.get(0).getX().get()*100.0)/100.0;
       double centroidY = Math.round(centroids.get(0).getY().get()*100.0)/100.0;

       double dataPointX = Math.round(commentCount_Score.getX().get()*100.0)/100.0;
       double dataPointY = Math.round(commentCount_Score.getY().get()*100.0)/100.0;

       double minDist = Math.round(Math.sqrt(Math.pow(dataPointX-centroidX,2)+
                                  Math.pow(dataPointY-centroidY,2))*100.0)/100.0;


      for(int i = 1; i < centroids.size(); i++){

        centroidX = Math.round(centroids.get(i).getX().get()*100.0)/100.0;
        centroidY = Math.round(centroids.get(i).getY().get()*100.0)/100.0;

        double next = Math.round(Math.sqrt(Math.pow(dataPointX-centroidX,2)+
                                Math.pow(dataPointY-centroidY,2))*100.0)/100.0;

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
