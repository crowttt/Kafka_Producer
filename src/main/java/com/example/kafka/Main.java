package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {

        String brokers="", topic="";
        int records=0, recordSize=0;

        int argsLength = args.length;
        for(int i=0 ; i<argsLength ; i++){
            if(args[i].equals("--brokers")){
                brokers = args[i+1];
            }
            if(args[i].equals("--topic")){
                topic = args[i+1];
            }
            if(args[i].equals("--records")){
                records = Integer.parseInt(args[i+1]);
            }
            if(args[i].equals("--recordSize")){
                recordSize = Integer.parseInt(args[i+1]);
            }
        }

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        prop.put(ProducerConfig.ACKS_CONFIG,"1");
        prop.put("retries", 0);
        prop.put("batch.size", 16384);
        prop.put("linger.ms", 1);
        prop.put("buffer.memory", 33554432);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for(int i=0;i<records;i++){
            String  key = String.format("key-%010d",i);
            String  value = String.format("value-%010d",i);
            for(int j=0; j< recordSize; ++j)
                value += '\0';
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }
}
