import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColorApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        KStreamBuilder builder = new KStreamBuilder();
        // 1 - stream from Kafka

        KStream<String, String> stream = builder.stream(Serdes.String(), Serdes.String(), "favourite-colour-input");
        // KTable since user can change opinion about is favourite color - allows update
        KStream<String, String> favoriteColoursUser = stream
                // 2 - filter only if contains 1 comma
                .filter((key,value) -> value.contains(","))
                // 3 - assign key
                .selectKey((key,value) -> value.split(",")[0].toLowerCase())
                // 4 - assign value
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // 5 - filter admissible colors
                .filter((user_id, colour) -> Arrays.asList("blue", "green", "red").contains(colour));
                // 6 - group by key before aggregation

        favoriteColoursUser.to("favourite-colours-users");

        KTable<String, String> streamColours = builder.table(Serdes.String(), Serdes.String(), "favourite-colours-users");
        KTable<String, Long> coloursCount = streamColours
                .groupBy((user,colour) -> new KeyValue<>(colour, user))
                .count("ColoursCount");

        // 7 - to in order to write the results back to kafka
        coloursCount.to(Serdes.String(), Serdes.Long(),"favourite-colour-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();
        streams.start();



        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        while(true){
            System.out.println(streams.toString());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }


    }
}
