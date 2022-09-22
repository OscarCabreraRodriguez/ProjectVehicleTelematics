package master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class SpeedRadar {
    public static SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> getSpeedRadar(DataStreamSource<String> source ){
        return source.map(new MapFunction<String, Tuple6<Integer, Integer,Integer,Integer, Integer, Integer>>() {
            @Override
            //We map the source data getting the desired variables Time, VID, XWay, Seg, Dir, Spd
            public Tuple6<Integer, Integer, Integer, Integer,Integer,Integer> map(String in) throws Exception {
                String[] fieldArray = in.split(",");
                Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out =
                        new Tuple6(Integer.parseInt(fieldArray[0]),Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[3]),
                                Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[2]));
                return out;
            }
        }).filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            //We filter the values over 90 mph
            public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> inputTuple) throws Exception {
                return inputTuple.f5>90;
            }
        });

    }
}
