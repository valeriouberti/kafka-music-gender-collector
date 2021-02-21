package it.ubetorbi.streams.producer;


import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@ApplicationScoped
public class ValuesGenerator {

    private final static Logger LOGGER = Logger.getLogger(ValuesGenerator.class);

    private Random random = new Random();


    private List<MusicGender> genders = Collections.unmodifiableList(
            Arrays.asList(new MusicGender(1, "pop", 10),
                    new MusicGender(2, "rock", 20),
                    new MusicGender(3, "rap", 30),
                    new MusicGender(4, "jazz", 5)
                    )
    );

    @Outgoing("num-of-plays")
    public Multi<Record<Integer,String>> generate(){

        return Multi.createFrom().ticks().every(Duration.ofMillis(500))
                .onOverflow().drop()
                .map(t -> {
                    MusicGender gender = genders.get(random.nextInt(genders.size()));
                    int numOfPlays = BigDecimal.valueOf(random.nextGaussian() * 15 +
                            gender.avarageOfPlays).setScale(1, RoundingMode.HALF_UP)
                            .intValue();
                    LOGGER.infov("Gender {0} - numOfPlays {1}", gender, numOfPlays);
                    return Record.of(gender.id, Instant.now() + ";" + numOfPlays);
                });
    }

    @Outgoing("genders")
    public Multi<Record<Integer,String>> genders(){
        return Multi.createFrom().items(genders.stream()
        .map(g -> Record.of(g.id, "{ \"id\" : " + g.id +
                        ", \"name\" : \"" + g.name + "\" }"))
        );
    }





    private static class MusicGender {
        private int id;
        private String name;
        private int avarageOfPlays;

        public MusicGender(int id, String name, int avarageOfPlays) {
            this.id = id;
            this.name = name;
            this.avarageOfPlays = avarageOfPlays;
        }
    }

}
