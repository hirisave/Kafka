package com.seanhirisave.batcave.kafka1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  // by providing the key we make sure the key always goes to the same partition
  public static void main(String[] args) throws ExecutionException, InterruptedException {

    final Logger logger = LoggerFactory.getLogger ( ProducerDemoKeys.class );
    String bootstrapServers ="127.0.0.1:9092";

    //   create producer properties
    Properties properties = new Properties (  );
    properties.setProperty ( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
    properties.setProperty ( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName () );
    properties.setProperty ( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName () );

    // create the producer
    KafkaProducer<String,String> producer = new KafkaProducer<> ( properties );

    for ( int i=0; i<10; i++ ){

      String topic = "first_topic";
      String value ="Hello World"+i;
      String key ="id_"+i;
      // create producer record
      ProducerRecord<String,String> record =
              new ProducerRecord<> ( topic, key, value);
      logger.info ( "key: "+ key );
      //id_1 partition 0
      //id-2 partition 0
      //id-3 partition 0
      //id-4 partition 2
      //id-5 partition 2
      //id-6 partition 0
      //id-7 partition 2
      //id-8 partition 1
      //id-9 partition 2

      //send data - asynchronous
      producer.send ( record, ( recordMetadata, e ) -> {
        //executes everytime a record is successfully sent or an exception is thrown
        if (e == null){
          // the record was successfully sent
          logger.info ( "Received new metadata.\n" +
                  "Topic: "+ recordMetadata.topic () +"\n"+
                  "Partition: "+ recordMetadata.partition () +"\n"+
                  "Offset: "+ recordMetadata.offset () +"\n"+
                  "Timestamp: "+ recordMetadata.timestamp () +"\n"
          );
        }else{
          logger.error ( "Error while producing to topic", e );
        }
      } ).get ();//block the send to make it synchronous :bad practise
    }

    //flush data and close producer
    producer.flush ();
    producer.close ();


  }
}
