package test;

import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import AirMap.AirMap;

/**
 * Created by honey on 17. 4. 28.
 */
public class Step2Bolt extends BaseRichBolt {
    private OutputCollector collector;

    static private ObjectArray emit_data = null;
    static private MWNumericArray n = null;
    static private MWNumericArray th = null;
    static private MWNumericArray region_n = null;
    static private Object[] result_step1_1 = null;
    static private Object[] result_step1_2 = null;
    static private Object[] result_step1_3 = null;
    static private Object[] result_step2 = null;
    static private Object[] result_step2_1 = null;
    static private Object[] result_step2_2 = null;
    static private Object[] result_step2_3 = null;
    static private Object[] result_step3 = null;
    static private Object[] bld3d = null;
    static private AirMap airMap = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

    }

    public void execute(Tuple tuple) {
        try {
            result_step2 = airMap.step2(1,region_n);
            for (int i = 1; i <=51; i++){
                n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
                System.out.println("### Step2 Start ###");
                result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);
                result_step2_2 = airMap.step2_2(2, result_step2_1[0], 0.1, result_step2[0]);
                result_step2_3 = airMap.step2_3(result_step1_2[0], result_step1_2[3], result_step2[0],
                        result_step2_2[0], n, region_n);
            }
        }
        catch (Exception e){
            System.out.println("Exception : " + e.toString());
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("step2"));
    }
}
