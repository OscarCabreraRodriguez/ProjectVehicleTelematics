/*
	Óscar Cabrera Rodríguez 
	Pau Rodríguez Inserte
 */

package master;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Skeleton code for the datastream walkthrough
 */
public class VehicleTelematics {

	public static void main(String[] args) throws Exception {
		//Set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//Get the input file and output path
		String input_file="", output_directory="";

		if (args.length==2)
		{
			input_file= args[0];
			output_directory = args[1];
		}
		else{
			throw new Exception("Wrong number of arguments. Please specify the input and output file. Use the following command \n " +
					"flink run -p 3 -c master.VehicleTelematics target/$YOUR_JAR_FILE $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER ");
		}

		//Read and load the input file
		DataStreamSource<String> source = env.readTextFile(input_file);

		//Exercise 1 speed radar
		SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> speedRadar = new SpeedRadar().getSpeedRadar(source);
		speedRadar.writeAsCsv(output_directory+"/speedfines.csv",FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		//Exercise 2 average speed control
		//Set the time characteristic as event time because we want the times of when an event is generated
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//Map the csv input file with the corresponding types
		DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> data = source.
				map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
					public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
						String[] fieldArray = in.split(",");
						Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8(Integer.parseInt(fieldArray[0]),Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[2]),
								Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[4]),Integer.parseInt(fieldArray[5]),
								Integer.parseInt(fieldArray[6]), Integer.parseInt(fieldArray[7]));
						return out;
					}
				});

		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>>  speedAvgControl =
				new AverageSpeedControl().getAverageSpeedControl(data);
		speedAvgControl.writeAsCsv(output_directory+"/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		//Exercise 3 Accident reporter
		SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>  accidentReporter =
				new AccidentReporter().getAccidentReporter(data);
		accidentReporter.writeAsCsv(output_directory+"/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		//Execute the environment
		try{
			env.execute("Vehicle Telematics");
		}
		catch (Exception e){
			e.printStackTrace();
		}

	}


}
