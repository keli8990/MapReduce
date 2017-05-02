package join;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This mapper is used to filter place names containing a particular country name
 * 
 * input format:
 * place_id \t woeid \t latitude \t longitude \t place_name \t place_type_id \t place_url
 * 
 * output format:
 * place_id \t place_url
 * 
 * The country name is stored as a property in the job's configuration object.
 * 
 * The configuration object can be obtained from the mapper/reducer's context object
 * 
 * Because all calls to the map function needs to use the country name value, we save it
 * as an instance variable countryName and set the value of it in the setup method. 
 * The setup method is called after the mapper is created. It is before any call of the
 * first map method. 
 * 
 * @see PlaceFilterDriver
 * @author Ying Zhou
 *
 */
public class PlaceFilterMapper extends Mapper<Object, Text, Text, Text> {

	private Text placeId= new Text(), placeUrl = new Text();
	
	
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 7){ // a not complete record with all data
			return; // don't emit anything
		}
		String place = dataArray[6]; 
		String placeTypeId = dataArray[5]; 
		if (placeTypeId.indexOf("7")>=0 ||placeTypeId.indexOf("22")>=0){
			String[] dataArray_palcename = place.toString().split("/");
		if(dataArray_palcename.length > 3){
				place = "/"+dataArray_palcename[3] + "/"+dataArray_palcename[2] + "/"+dataArray_palcename[1];
			}
			else{
			//	if("".equals(dataArray_palcename[3])||dataArray_palcename.length < 4){
					 place = "/"+dataArray_palcename[2] + "/"+dataArray_palcename[1] + "/"+dataArray_palcename[0];	
			//	}else{
			//	 place = "/"+dataArray_palcename[3] + "/"+dataArray_palcename[2] + "/"+dataArray_palcename[1];
			//	}
			}
			placeId.set(dataArray[0]);
			placeUrl.set(place);
			context.write(placeId,placeUrl);
		}
		
	}

}

