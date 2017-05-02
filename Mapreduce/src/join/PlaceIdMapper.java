package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PlaceIdMapper extends Mapper<Object, Text, Text, Text> {
	private Hashtable <String, String> placeTable = new Hashtable<String, String>();
	private Hashtable <String, String> photoTable = new Hashtable<String, String>();

	private Text keyOut = new Text(), valueOut = new Text();
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 

	
	public void setPlaceTable(Hashtable<String,String> place){
		placeTable = place;
	}
	
	public void setPhotoTable(Hashtable<String,String> photo){
		photoTable = photo;
	}
	
	
	public void setup(Context context)
		throws java.io.IOException, InterruptedException{
		
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;
			
			String[] tokens;
			
			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try {
				while ((line = placeReader.readLine()) != null) {
					tokens = line.split("\t");
					placeTable.put(tokens[0], tokens[1]); // 0->placeID 1->url
				}
			} 
			finally {
				placeReader.close();
			}
			
		}
		
		if (cacheFiles != null && cacheFiles.length > 1) {
			String line_0;
			
			String[] tokens_0;
			
			BufferedReader photoReader = new BufferedReader(new FileReader(cacheFiles[1].toString()));
			try {
				while ((line_0 = photoReader.readLine()) != null) {
					tokens_0 = line_0.split("\t");
					photoTable.put(tokens_0[0], tokens_0[1]); // 0->placeID 1->url
				}
			} 
			finally {
				photoReader.close();
			}
		}
	}
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); 
		if (dataArray.length < 5){ 
			return; 
		}
		
		String placeId = dataArray[4];
		String placeName = placeTable.get(placeId);
		String tagString = dataArray[2];
		String frequence = null;
		if(placeName !=null && photoTable.containsKey(placeName)){
			frequence = photoTable.get(placeName);
				if (tagString.length() > 0 && placeName !=null){
					String[] tagArray = tagString.split(" ");
					for(String tag: tagArray) {
						if (asciiEncoder.canEncode(tag)){
							keyOut.set(placeName+"\t"+frequence);
							valueOut.set(tag);
							context.write(keyOut, valueOut);

						}
					}
				}
		}
		
		
	}

}

