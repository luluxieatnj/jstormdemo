package com.xll.js;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class LocalPology {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("out-word", new WordSpout(),1);
        builder.setBolt("word-count", new CountBolt(),1).shuffleGrouping("out-word");
        //本地模式:本地提交
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        cluster.submitTopology("firstTopo", conf, builder.createTopology());
        //一定要等待足够的时间，否则程序没来得及运行就已经结束了，程序启动需要消耗时间
        Thread.sleep(30000);
        cluster.killTopology("firstTopo");
        cluster.shutdown();
    }
}
