import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Hashtable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class UserBadgesClusteringMapper extends Mapper<Object, Text, DoubleDoublePair, DoubleDoublePair> {

  private ArrayList<DoubleDoublePair> centroids = new ArrayList<DoubleDoublePair>();
  private DoubleDoublePair user_badges;

  @Override
	public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

       int centroidIndex = 0;
       String[] line = data.toString().split("\t"); //Split data into tab space.

       try{
         //Retrieve the User Age and Badges from input data.
         user_badges = new DoubleDoublePair(Double.parseDouble(line[0]),
                                            Double.parseDouble(line[1]));

         double centroidX = centroids.get(0).getX().get(); //Get X axis of centroid.
         double centroidY = centroids.get(0).getY().get(); //Get Y axis of centroid.

         double dataPointX = ((user_badges.getX().get()) - 13)/81;      //Get X axis of data point and normalise it.
         double dataPointY = ((user_badges.getY().get()) - 29)/212342;  //Get Y axis of data point and normalise it.

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
      //Emit the centroid and the UserAge/Badges data point.
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
}
