package mx.iteso.desi.cloud.hw2;

import mx.iteso.desi.cloud.*;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.InterruptedException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

public class GeocodeMapper extends Mapper<LongWritable, Text, Text, GeocodeWritable> {

	private ArrayList<Geocode> citites;

	protected void setup(Context context) throws IOException, InterruptedException{

		//get configuration
		Configuration conf = context.getConfiguration();
		citites = new ArrayList<Geocode>();

		//get values for a key "city", and parse it into Geocodes
		for (String city : conf.get("city").split(";")){
			String [] dataCity = city.split(",");
			citites.add(new Geocode(dataCity[0], Double.valueOf(dataCity[1]), Double.valueOf(dataCity[2])));
		}
	}

	public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException{

					//Text, which is the value, contains the data we are interested in, the <A><B><C> relationship. Parse it to triple
					Triple currentTriple = ParseTriple.parseTriple(text.toString());

					if(currentTriple != null){
						GeocodeWritable value;
						//key is always article :
						Text key = new Text(currentTriple.get(0));
	
						//Is a valid geocode or image?
						if(currentTriple.get(1).contains("http://xmlns.com/foaf/0.1/depiction")){
							//It is an image
							value = new GeocodeWritable(currentTriple.get(2), 0.0, 0.0);
							context.write(key, value);
	
						} else if (currentTriple.get(1).contains("http://www.georss.org/georss/point")){
							//It is a geocode
							//Parse the coordinates, and generate a Geocode
							Double[] lat_long = ParserCoordinates.parseCoordinates(currentTriple.get(2));
							//Is this geocode in the range of 5km of main citites?
							for (Geocode city : citites){
								if(city.getHaversineDistance(lat_long[0], lat_long[1]) <= 5000){
									value = new GeocodeWritable("geocode", lat_long[0], lat_long[1]);
									context.write(key, value);
								}
							}
	
						} else {
				
						}
					}


	}  
}
