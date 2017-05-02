package test;

import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
//import ycNoise2.Class1;
//import package2.Class1;
import AirMap.AirMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;


public class Step3Bolt extends BaseRichBolt {
    private OutputCollector collector;
    private int index = 0;

    static private ObjectArray emit_data = null;
    static public MWNumericArray n = null;
    static public MWNumericArray region_n = null;
    static public Object[] result_step1_1 = null;
    static public Object[] result_step1_2 = null;
    static private Object[] result_step1_3 = null;
    static private Object[] result_step2 = null;
    static private Object[] result_step2_1 = null;
    static private Object[] result_step2_2 = null;
    static private Object[] result_step2_3 = null;
    static private Object[] result_step3 = null;
    static public Object[] bld3d = null;
    static public AirMap airMap = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

//        System.load("/home/yechan/Downloads/newNoiseStorm2/javabuilder.jar");
//        System.load("/home/yechan/Downloads/newNoiseStorm2/ycNoise2.jar");
        try {
            emit_data = new ObjectArray();


            System.out.println("*** result bolt - Create Class1 AirMap ***");
            this.airMap = new AirMap();

            int o=0;
                System.out.println("region_n = " + o);
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);

                result_step1_1 = airMap.step1_1(5);
                // output[5] - xi, yi, grd_spc, gnd_rng, gnd_grd

                result_step1_2 = airMap.step1_2(5);
                // output[2] - nz_rng, cmap

                bld3d= airMap.step2_1(1,region_n);
                // output[1] - bld3d

            /*airMap.step1_5(region_n);
            airMap.step3(result_step1_1[0],result_step1_1[1],result_step1_1[2],
                    result_step1_1[4],bld3d[0],region_n);
                    */
        }
        catch (Exception e)
        {
            System.out.println("Exception: " + e.toString());
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // not need to create Output Stream;
    }

    public void execute(Tuple tuple) {

        try {
            emit_data = (ObjectArray)tuple.getValueByField("result");

            if(emit_data.getFlag()==1) {

                //Object[] nz_gnd_gi = emit_data.getValue();
                airMap.step1_3(result_step1_2[1], result_step1_2[3], result_step1_2[4],region_n);


            }
            else if(emit_data.getFlag()==2)
            {
                Object[] nz_bld_gi = emit_data.getValue();

                //airMap.step2_3(result_step1_2[0], result_step1_2[3],result_step2[0],
                //        resu[2],region_n);

            }
            index++;
        }
        catch (Exception e)
        {
            System.out.println("Bolt2 Exception: " + e.toString());
        }

    }

    @Override
    public void cleanup() {
        System.out.println("------- FINAL COUNT -------");
        System.out.println("FINAL COUNT IS :  " + index);
        System.out.println("---------------------------");
    }
}

