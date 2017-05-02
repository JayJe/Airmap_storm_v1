package test;

import com.mathworks.toolbox.javabuilder.MWNumericArray;
//import ycNoise2.Class1;
//import package2.Class1;
import AirMap.AirMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


public class AirTopology {


    static public MWNumericArray n = null;
    static public MWNumericArray region_n = null;
    static public Object[] result_step1_1 = null;
    static public Object[] result_step1_2 = null;
    static public Object[] bld3d = null;
    private static AirMap airMap=null;

    public static void main(String[] args) {


        System.out.println("### Create Topology Builder ###");
        TopologyBuilder builder = new TopologyBuilder();

        /*
        System.out.println("***** 1 *****");
        AirSpout spout = new AirSpout();

        System.out.println("***** 2 *****");
        AirBolt airBolt = new AirBolt();

        System.out.println("***** 3 *****");
        Step3Bolt step3Bolt = new Step3Bolt();


        System.out.println("***** 4 *****");
        */
        //parallelism_hint(Thread) and setNumTask(Component Number)
        builder.setSpout("spout", new AirSpout(), 2); // Spout 연결
        //builder.setBolt(NOISE_BOLT_ID, noiseBolt).shuffleGrouping(AIR_SPOUT_ID); // Spout -> SplitBolt
        builder.setBolt("step1",new AirBolt(), 4)
                .shuffleGrouping("spout"); // Spout -> SplitBolt
        builder.setBolt("step2", new Step2Bolt(), 4)
                .shuffleGrouping("spout"); // SplitBolt -> CountBolt
        builder.setBolt("Step3", new Step3Bolt(), 4)
                .shuffleGrouping("spout");


        LocalCluster cluster = new LocalCluster();


        Config config = new Config();
        //config.put("java.library.path","/home/yechan/Downloads/newNoiseStorm2/ycNoise2.jar");
        config.registerSerialization(ObjectArray.class);

        cluster.submitTopology("AirTopology", config, builder.createTopology());

        try { Thread.sleep(1000 * 660); } catch (InterruptedException e) { } // waiting 10s
        cluster.killTopology("AirTopology");
        cluster.shutdown();
    }
}
