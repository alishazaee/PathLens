package org.shazaei.generator;

import java.util.concurrent.ThreadLocalRandom;

public class RawLogGenerator {

    public void randomLog() {

    }

    private int randomNumberGenerator(){
        return ThreadLocalRandom.current().nextInt(1,1000);
    }
}
