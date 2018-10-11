package mx.iteso.desi.cloud.hw2;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.Iterable;

import mx.iteso.desi.cloud.GeocodeWritable;

public class GeocodeReducer extends Reducer<Text, GeocodeWritable, Text, Text> {

  /* TODO: Your reducer code here */
  public void reduce(Text text, Iterable<GeocodeWritable> geocodeWritable, Context context) throws IOException, InterruptedException{

    String geoString = "";
    String imageString = "";
    for(GeocodeWritable geocode : geocodeWritable){
      if(geocode.getName().toString().equals("geocode")){
        geoString = geocode.toString();
      } else {
        imageString = geocode.getName().toString();
      }
    }
    if(!geoString.equals("") && !imageString.equals("")){
      context.write(new Text(""), new Text(geoString + "\t" + text.toString() + "\t" + imageString));
    }
  }
}
