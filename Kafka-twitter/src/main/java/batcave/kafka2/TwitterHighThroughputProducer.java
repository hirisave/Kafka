package batcave.kafka2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TwitterHighThroughputProducer {

    Logger logger = LoggerFactory.getLogger ( TwitterHighThroughputProducer.class.getName ());
    String consumerKey = "9cfNzvwY5l5EpuWmdzVEgaQex";
    String consumerSecret = "CMMZLSZWVDrUZ2Bc3QiePf8cFEeWirKyGJyI2c95FtiestGp3N";
    String token = "1289958932467920896-MMPWiPSX2Ot1YoQjAAQuxn8HLI3XHt";
    String secret = "Vs34XhjonyzA8FPenUvD3uwGXA155n3EDuYHHpxGApUuQ";
    List<String> terms = Lists.newArrayList("latvia");

    public TwitterHighThroughputProducer (){}
    public static void main(String[] args) {
        new TwitterHighThroughputProducer ().run ();
    }

    public void run(){
        logger.info ( "Setup" );
        //Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<> (1000);

        // create a Twitter client
        Client client = createTwitterClient (msgQueue);
        client.connect(); // Attempts to establish a connection.

        // create a kafka producer
        KafkaProducer<String ,String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime ().addShutdownHook ( new Thread ( ()->{
            logger.info ( "stopping application..." );
            logger.info ( "shutting down twitter client..." );
            client.stop ();
            logger.info ( "closing producer..." );
            producer.close ();
            logger.info ( "voila!!!!" );
        } ) );
        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll (5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace ( );
                client.stop ();
            }
            if ( msg != null){
                logger.info ( msg );
                producer.send ( new ProducerRecord<> ( "twitter_tweets", null, msg ), new Callback ( ) {
                    @Override
                    public void onCompletion ( RecordMetadata recordMetadata, Exception e ) {
                        if (e != null) {
                            logger.error ( "Something Bad Happened", e );
                        }
                    }
                } );
            }
        }logger.info ( "End of application" );

    }

    private KafkaProducer<String, String> createKafkaProducer ( ) {
        String bootstrapServers ="127.0.0.1:9092";

        //   create producer properties
        Properties properties = new Properties (  );
        properties.setProperty ( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        properties.setProperty ( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName () );
        properties.setProperty ( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName () );

        //create safe producer
        properties.setProperty ( ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true" );
        properties.setProperty ( ProducerConfig.ACKS_CONFIG,"all" );
        properties.setProperty ( ProducerConfig.RETRIES_CONFIG, Integer.toString ( Integer.MAX_VALUE ));
        properties.setProperty ( ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5" );// kafka >=kafka1.1 so we can keep this as 5. Use 1 otherwise

        // create high throughput producer(at the expense of CPU usage bit of a latency)
        properties.setProperty ( ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy" );
        properties.setProperty ( ProducerConfig.LINGER_MS_CONFIG,"20" );
        properties.setProperty ( ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString ( 32*1024 ) ); // 32KB batch size
        // create the producer
        return new KafkaProducer<> ( properties );
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        //Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts ( Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1 (
                        consumerKey,
                        consumerSecret,
                        token,
                        secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor (msgQueue));
        return builder.build();
    }

}
