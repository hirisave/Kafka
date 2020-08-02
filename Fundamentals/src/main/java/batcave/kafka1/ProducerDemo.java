package batcave.kafka1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
  public static void main(String[] args) {
    String bootstrapServers ="127.0.0.1:9092";

    //   create producer properties

    Properties properties = new Properties (  );
    properties.setProperty ( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
    properties.setProperty ( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName () );
    properties.setProperty ( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName () );

    // create the producer

    KafkaProducer<String,String> producer = new KafkaProducer<> ( properties );

    // create producer record

    ProducerRecord<String,String> record =
            new ProducerRecord<> ( "first_topic","Hello World" );

    //send data - asynchronous
    producer.send ( record );

    //flush data and close producer
    producer.flush ();
    producer.close ();


  }
}
