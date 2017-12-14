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

public class PostCommentScoreClusteringMapper extends Mapper<Object, Text, DoubleDoublePair, DoubleDoublePair> {
  private Map<String,String> userMap;
  private ArrayList<DoubleDoublePair> centroids = new ArrayList<DoubleDoublePair>();
  private Hashtable<Integer, Integer> userAgeTable;
  private DoubleDoublePair commentCount_Score;



  @Override
	public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

       int centroidIndex = 0;
       userMap = transformXmlToMap(data.toString());
       try{

       if(userAgeTable.contains(userMap.get("UserId"))){
       	int age=userAgeTable.get(userMap.get("UserId"));

       commentCount_Score = new DoubleDoublePair((double) age,
                                                 Double.parseDouble(userMap.get("BountyAmount")));
       double centroidX = Math.round(centroids.get(0).getX().get()*100.0)/100.0;
       double centroidY = Math.round(centroids.get(0).getY().get()*100.0)/100.0;

       double dataPointX = Math.round(commentCount_Score.getX().get()*100.0)/100.0;
       double dataPointY = Math.round(commentCount_Score.getY().get()*100.0)/100.0;

       double minDist = Math.round(Math.sqrt(Math.pow(dataPointX-centroidX,2)+
                                  Math.pow(dataPointY-centroidY,2))*100.0)/100.0;

      System.out.println(commentCount_Score);

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
      }
    }catch(NullPointerException e){}

	}

protected void setup(Context context) throws IOException, InterruptedException {

        userAgeTable = new Hashtable<Integer, Integer>();

        // Single cache file, so we only retrieve that URI
        URI fileUri = context.getCacheFiles()[0];

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(fileUri));

        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line = null;
        try {
            // Discard the headers line
            br.readLine();

            while ((line = br.readLine()) != null) {

                userMap = transformXmlToMap(line.toString());

                try{
                userAgeTable.put(Integer.parseInt(userMap.get("Id")),
                                 Integer.parseInt(userMap.get("Age")));
                }catch(Exception e){}

            }
            br.close();
        } catch (IOException e1) {
        }

        super.setup(context);
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