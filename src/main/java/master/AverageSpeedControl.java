package master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AverageSpeedControl {
    static final double MS_TO_MPH = 2.2369356;

    public SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>>
    getAverageSpeedControl(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> data ){
        return data.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    //First we filter the segment values that are between 52 and 56
                    public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple_filter) throws Exception {
                        return tuple_filter.f6>=52 && tuple_filter.f6<=56;
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        //we assign a timestamp in order to process the times later in the window
                        return element.f0 * 1000;
                    }
                })
                //we key the data by the following parameters VID, Xway and Dir to group the different cars and the direction in the highway
                .keyBy(1,3,5)
                //We create an event window of 30 seconds because every 30 seconds we get a car report
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow>() {
                    @Override
                    //In this function we get the first time and the last time of the segment and given the position of the car calculate the average speed
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer , Integer, Integer, Integer, Integer>> input,
                                      Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {
                        List<Integer> times = new ArrayList<Integer>();
                        List<Integer> positions = new ArrayList<Integer>();
                        int max_time = 0, min_time = 0;
                        int min_pos=0, max_pos=0;
                        double average=0;
                        boolean seg52=false,seg56=false;
                        for (Tuple8 element : input) {
                            if ((int) element.f6 == 52) {
                                seg52 = true;
                                times.add((int) element.f0);
                                positions.add((int) element.f7);
                            } else if ((int) element.f6 == 56) {
                                seg56 = true;
                                times.add((int) element.f0);
                                positions.add((int) element.f7);
                            }
                            if (seg52 && seg56) {
                                max_time = Collections.max(times);
                                min_time = Collections.min(times);
                                max_pos= Collections.max(positions);
                                min_pos= Collections.min(positions);
                                average = (MS_TO_MPH*(max_pos-min_pos))/(max_time-min_time);
                            }
                        }
                        //We return the tuple with the following parameters Time1, Time2, VID, XWay, Dir, AvgSpd
                        Tuple6 result=new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>
                                (min_time, max_time, tuple.getField(0), tuple.getField(1), tuple.getField(2), average);
                        out.collect(result);
                    }
                }).filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>>() {
                    @Override
                    //We filter the tuples with an average speed greater than 60
                    public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Double> tuple_filter) throws Exception {
                        return tuple_filter.f5>60;
                    }
                });

    }
}
