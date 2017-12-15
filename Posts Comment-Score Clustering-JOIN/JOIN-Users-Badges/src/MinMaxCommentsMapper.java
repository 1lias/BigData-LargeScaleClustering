import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCommentsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
  private Map<String,String> userMap;
  private Hashtable<String, String> userAgeTable;

  @Override
	public void map(Object key, Text data, Context context)
                                      throws IOException, InterruptedException {

     userMap = transformXmlToMap(data.toString());
     try{

       if(userAgeTable.containsKey(userMap.get("UserId"))){
         System.out.println("IF");
       	String age=userAgeTable.get(userMap.get("UserId"));
        System.out.println("IF and age"+age);
          context.write(new IntWritable(Integer.parseInt(age)), new IntWritable(1));
       }else{
                System.out.println("else "+userMap.get("UserId"));
       }
     }catch(Exception e){    System.out.println("MES TON CATCHER KURILE");}

	}

  protected void setup(Context context) throws IOException, InterruptedException {
          userAgeTable = new Hashtable<String, String>();

          // Single cache file, so we only retrieve that URI
          URI fileUri = context.getCacheFiles()[0];

          FileSystem fs = FileSystem.get(context.getConfiguration());
          FSDataInputStream in = fs.open(new Path(fileUri));

          BufferedReader br = new BufferedReader(new InputStreamReader(in));

          String line = null;

              // Discard the headers line
              br.readLine();

              while ((line = br.readLine()) != null) {

                  userMap = transformXmlToMap(line.toString());

          try {

                  userAgeTable.put(userMap.get("Id"),
                                   userMap.get("Age"));
                  System.out.println("EVALA: " + userAgeTable.get(userMap.get("Id")));


                       } catch (Exception e1) {
                        }
                      }
              br.close();

          System.out.println("EXIT SETUP");
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
