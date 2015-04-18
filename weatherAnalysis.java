import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class weatherAnalysis {

  // First mapper class
  // Input: input text file 
  // Output key: StationId+Year+Month, Output value: Temperature+Wind
  public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String inputLine = value.toString();
      if (inputLine.substring(0, 1).matches("[0-9]")) {
        String arr[] = inputLine.split("[ ,]+");
        String keyToReducer = arr[0] + arr[2].substring(0, 6);
        double temperature = Double.parseDouble(arr[3].toString());
        double wind = Double.parseDouble(arr[12].toString());
        context.write(new Text(keyToReducer), new Text(temperature + "," + wind));
      }
    }
  }

  // First reducer class
  // Input key: StationId+Year+Month, Input value: Temperature+Wind
  // Output key: StationId+Year+Month, Output value: minTemperature+maxTemperature+minWind+MaxWind
  public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      int tempValuesCount = 0;
      double sumTemp = 0.0;
      double maxTemp = 0.0;
      double minTemp = 1000.0;

      int windValuesCount = 0;
      double sumWind = 0.0;
      double maxWind = 0.0;
      double minWind = 1000.0;

      double tempMissingValue = 9999.9;
      double tempMinimumValue = 0.0;
      double windMissingValue = 999.9;

      for (Text val : values) {
        String tempwind[] = val.toString().split(",");
        double temp = Double.parseDouble(tempwind[0].toString());
        double wind = Double.parseDouble(tempwind[1].toString());

        if (temp != tempMissingValue && temp >= tempMinimumValue) {
          if (temp < minTemp)
            minTemp = temp;
          if (temp > maxTemp)
            maxTemp = temp;
          sumTemp += temp;
          tempValuesCount++;
        }

        if (wind != windMissingValue) {
          if (wind < minWind)
            minWind = wind;
          if (wind > maxWind)
            maxWind = wind;
          sumWind += wind;
          windValuesCount++;
        }
      }
      context.write(key, new Text("MinTemp = " + minTemp + " MaxTemp = " + maxTemp + " AvgTemp = " + String.format("%.2f", (sumTemp/tempValuesCount)) + " MinWind = " + minWind + " MaxWind = " + maxWind + " AvgWind = " + String.format("%.2f", (sumWind/windValuesCount))));
    }
  }

  // Second mapper class
  // Input key: StationId+Year+Month, Input value: minTemperature+maxTemperature+minWind+MaxWind
  // Output key: Year, Output value: minTemperature+maxTemperature+minWind+MaxWind+StationId+Year
  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String inputLine = value.toString();
      String arr[] = inputLine.split("[ ]+");

      String keyToReducer = arr[0].substring(6, 10);
      double minTemp = Double.parseDouble(arr[2].toString());
      double maxTemp = Double.parseDouble(arr[5].toString());
      double minWind = Double.parseDouble(arr[11].toString());
      double maxWind = Double.parseDouble(arr[14].toString());
      String stnId = arr[0].substring(0, 6);
      
      context.write(new Text(keyToReducer), new Text(minTemp + "," + maxTemp + "," + minWind + "," + maxWind + "," + stnId + "," + keyToReducer));
    }
  }

  // Second reducer class
  // Input key: Year, Input value: minTemperature+maxTemperature+minWind+MaxWind+StationId+Year
  // Output: Final output file
  public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      int count = 0;

      List<Double> minTempList = new ArrayList<Double>();
      List<Double> maxTempList = new ArrayList<Double>();
      List<Double> minWindList = new ArrayList<Double>();
      List<Double> maxWindList = new ArrayList<Double>();
      List<String> stnIdList = new ArrayList<String>();
      List<String> yearList = new ArrayList<String>();
      
      for (Text val : values) {
        String minmaxtempwind[] = val.toString().split(",");
        minTempList.add(Double.parseDouble(minmaxtempwind[0].toString()));
        maxTempList.add(Double.parseDouble(minmaxtempwind[1].toString()));
        minWindList.add(Double.parseDouble(minmaxtempwind[2].toString()));
        maxWindList.add(Double.parseDouble(minmaxtempwind[3].toString()));
        stnIdList.add(minmaxtempwind[4].toString());
        yearList.add(minmaxtempwind[5].toString());
        count++;
      }

      // min temp array and sort
      int[] flagArray = new int[count];

      double[] minTempArray = new double[count];
      for (int i = 0; i < minTempArray.length; i++) {
        minTempArray[i] = minTempList.get(i);
      }
      Arrays.sort(minTempArray);

      double[] sortedMinTempArray = new double[5];
      String[] sortedMinTempStnIdsArray = new String[5];
      
      for (int i = 0; i < 5; i++) {
        sortedMinTempArray[i] = minTempArray[i];
      }

      for (int k = 0; k < sortedMinTempArray.length; k++) {
        for (int m = 0; m < count; m++) {
          if(yearList.get(m).matches(key.toString()) && sortedMinTempArray[k] == minTempList.get(m) && flagArray[m] != 1) {
            sortedMinTempStnIdsArray[k] = stnIdList.get(m);
            flagArray[m] = 1;
            context.write(new Text("Minimum temperature in year " + key.toString() + " = "), new Text(Double.toString(sortedMinTempArray[k]) + ", recorded at StationID: " + sortedMinTempStnIdsArray[k]));
            break;
          }
        }
      }

      // max temp array and sort
      flagArray = new int[count];
      double[] maxTempArray = new double[count];
      for (int i = 0; i < maxTempArray.length; i++) {
        maxTempArray[i] = maxTempList.get(i);
      }
      Arrays.sort(maxTempArray);

      double[] sortedMaxTempArray = new double[5];
      String[] sortedMaxTempArrayStnIdsArray = new String[5];

      for (int i = count - 1, x = 0; i > count - 6; i--, x++) {
        sortedMaxTempArray[x] = maxTempArray[i];
      }

      for (int k = 0; k < sortedMaxTempArray.length; k++) {
        for (int m = 0; m < count; m++) {
          if (yearList.get(m).matches(key.toString()) && sortedMaxTempArray[k] == maxTempList.get(m) && flagArray[m] != 1) {
            sortedMaxTempArrayStnIdsArray[k] = stnIdList.get(m);
            flagArray[m] = 1;
            context.write(new Text("Maximum temperature in year " + key.toString() + " = "), new Text(Double.toString(sortedMaxTempArray[k]) + ", recorded at StationID: " + sortedMaxTempArrayStnIdsArray[k]));
            break;
          }
        }
      }

      // min wind array and sort
      flagArray = new int[count];
      double[] minWindArray = new double[count];
      for (int i = 0; i < minWindArray.length; i++) {
        minWindArray[i] = minWindList.get(i);
      }
      Arrays.sort(minWindArray);

      double[] sortedMinWindArray = new double[5];
      String[] sortedMinWindStnIdsArray = new String[5];

      for (int i = 0; i < 5; i++) {
        sortedMinWindArray[i] = minWindArray[i];
      }

      for (int k = 0; k < sortedMinWindArray.length; k++) {
        for (int m = 0; m < count; m++) {
          if (yearList.get(m).matches(key.toString()) && sortedMinWindArray[k] == minWindList.get(m) && flagArray[m] != 1) {
            sortedMinWindStnIdsArray[k] = stnIdList.get(m);
            flagArray[m] = 1;
            context.write(new Text("Minimum windspeed in year " + key.toString() + " = "), new Text(Double.toString(sortedMinWindArray[k]) + ", recorded at StationID: " + sortedMinWindStnIdsArray[k]));
            break;
          }
        }
      }

      // max wind array and sort
      flagArray = new int[count];
      double[] maxWindArray = new double[count];
      for (int i = 0; i < maxWindArray.length; i++) {
        maxWindArray[i] = maxWindList.get(i);
      }
      Arrays.sort(maxWindArray);

      double[] sortedMaxWindArray = new double[5];
      String[] sortedMaxWindArrayStnIdsArray = new String[5];

      for (int i = count - 1, x = 0; i > count - 6; i--, x++) {
        sortedMaxWindArray[x] = maxWindArray[i];
      }

      for (int k = 0; k < sortedMaxWindArray.length; k++) {
        for (int m = 0; m < count; m++) {
          if(yearList.get(m).matches(key.toString()) && sortedMaxWindArray[k] == maxWindList.get(m) && flagArray[m] != 1) {
            sortedMaxWindArrayStnIdsArray[k] = stnIdList.get(m);
            flagArray[m] = 1;
            context.write(new Text("Maximum windspeed in year " + key.toString() + " = "), new Text(Double.toString(sortedMaxWindArray[k]) + ", recorded at StationID: " + sortedMaxWindArrayStnIdsArray[k]));
            break;
          }
        }
      }
    }
  }


  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

    @SuppressWarnings("deprecation")
    Job job1 = new Job(conf, "firstJob");
    job1.setJarByClass(weatherAnalysis.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.setMapperClass(Map1.class);
    job1.setReducerClass(Reduce1.class);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("/user/hduser/tempoutput"));

    job1.waitForCompletion(true);

    @SuppressWarnings("deprecation")
    Job job2 = new Job(conf, "secondJob");
    job2.setJarByClass(weatherAnalysis.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.setMapperClass(Map2.class);
    job2.setReducerClass(Reduce2.class);
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job2, new Path("/user/hduser/tempoutput/"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

    job2.waitForCompletion(true);

  }
}