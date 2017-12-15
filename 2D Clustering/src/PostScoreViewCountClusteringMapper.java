import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Hashtable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class PostScoreViewCountClusteringMapper extends Mapper<Object, Text, DoubleDoublePair, DoubleDoublePair> {
  private ArrayList<DoubleDoublePair> centroids = new ArrayList<DoubleDoublePair>();
  private Map<String,String> postMap;
  private DoubleDoublePair postScore_ViewCount;



  @Override
	public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

    int centroidIndex = 0;
    postMap = transformXmlToMap(data.toString());  //Transform the User entry to Map.

    try{
      //Retrieve the Score and ViewCount from Post entry.
      postScore_ViewCount = new DoubleDoublePair(Double.parseDouble(postMap.get("Score")),
                                                 Double.parseDouble(postMap.get("ViewCount")));

      double centroidX = centroids.get(0).getX().get(); //Get X axis of centroid.
      double centroidY = centroids.get(0).getY().get(); //Get Y axis of centroid.

      double dataPointX = ((postScore_ViewCount.getX().get()) - -166.0)/6683;   //Get X axis of data point and normalise it.
      double dataPointY = ((postScore_ViewCount.getY().get()) - 0)/1677209;     //Get Y axis of data point and normalise it.

      //Calculate the distance using Euclidean Distance metric.
      double minDist = Math.sqrt(Math.pow(dataPointX-centroidX,2)+
                                 Math.pow(dataPointY-centroidY,2));

      //Loop through the three centroids.
      for(int i = 1; i < centroids.size(); i++){

        centroidX = centroids.get(i).getX().get();
        centroidY = centroids.get(i).getY().get();
        //Calculate the distance of centroid and post entry.
        double next = Math.sqrt(Math.pow(dataPointX-centroidX,2)+
                                Math.pow(dataPointY-centroidY,2));
        //Take the smallest distance of all centroids.
        if(Math.abs(next) < Math.abs(minDist)){
          minDist = next;
          centroidIndex = i;
        }
      }
      //Emit the centroid and the Post data point.
      context.write(centroids.get(centroidIndex),new DoubleDoublePair(dataPointX,dataPointY));
    }catch(NullPointerException e){}

	}

  /*************************************************************
   * Setup method used at the initialisation of Mapper to load
   * the centroids sent from the job configuration.
   ************************************************************/
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
