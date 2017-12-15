import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.Hashtable;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;

public class JoinUserBadgesMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

  private Hashtable<String, String> usersTable;  //Hashtable of users from the cached dataset.
  private Map<String,String> badgesMap;          //Map of badges extracted from XML

  @Override
  public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

      badgesMap = transformXmlToMap(data.toString());

      try{
        //Check if the badges dataset contains the UserId of
        //the corresponding user entry.
        if(usersTable.containsKey(badgesMap.get("UserId"))){

          String age = usersTable.get(badgesMap.get("UserId")); //Retrieve the age.
          //Emit the Age as key and 1 as value.
          context.write(new IntWritable(Integer.parseInt(age)), new IntWritable(1));

        }

      }catch(Exception e){}
    }

    /****************************************************************
     * Setup method used to load the Users data into Hashtable.
     ***************************************************************/
    protected void setup(Context context) throws IOException, InterruptedException {

      usersTable = new Hashtable<String, String>();

      // Single cache file, so we only retrieve that URI
      URI fileUri = context.getCacheFiles()[0];

      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataInputStream in = fs.open(new Path(fileUri));

      BufferedReader br = new BufferedReader(new InputStreamReader(in));

      String line = null;

      // Discard the headers line
      br.readLine();

      while ((line = br.readLine()) != null) {

          badgesMap = transformXmlToMap(line.toString());

          try {
            usersTable.put(badgesMap.get("Id"),
                             badgesMap.get("Age"));
           } catch(NullPointerException e){}
             catch(NumberFormatException e){}
      }

      br.close();

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
