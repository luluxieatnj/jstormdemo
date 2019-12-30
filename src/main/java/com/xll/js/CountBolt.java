package com.xll.js;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;


public class CountBolt implements IRichBolt {
    // 一定要 生成 一个 serialVersionUID，因为这些class 都是要经过序列化的
    private static final long serialVersionUID = 8740926838799779884L;
    Map<String, Integer> map = new HashMap<>();

    private volatile BufferedWriter bw = null;

    public CountBolt() {
        System.out.println("CountBolt:初始化.......................");
    }

    /**
     * prepare 在这里仅仅是启动了一个文件写的定时线程，每2秒将结果写到文件中，并清空map.
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.out.println("CountBolt.preoare().....................");
        try {
            bw = new BufferedWriter(new FileWriter("log.tex", true));
        } catch (IOException e) {
        }
    }

    /**
     * 收到 spout 发送来的 Word 进行统计
     */
    @Override
    public void execute(Tuple input) {
        System.out.println("CountBolt.execute(Tuple input)......");
        String word = input.getString(0);
        System.out.println("CountBolt.execute(Tuple input)......,word is " + word);

        if (map.get(word) == null) {
            map.put(word, 1);
        } else {
            map.put(word, map.get(word) + 1);
        }
        Set<Entry<String, Integer>> sets = map.entrySet();
        for (Entry<String, Integer> entry : sets) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            System.out.println("/////////////////// key = " + key + " : value = " + value);
            try {
                bw.write("/////////////////// key = " + key + " : value = " + value);
                bw.newLine();
            } catch (IOException e) {}
        }
    }

    /**
     * Topology 被 shutdown时会被触发的动作，我们可以用来做一些清理工作
     */
    @Override
    public void cleanup() {
        System.out.println("*******************public void cleanup().....");
        for (Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        map.clear();
        try {
            bw.flush();
            bw.close();
        } catch (IOException e) {

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}