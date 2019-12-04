package org.hillview.test.security;

import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.junit.Test;

public class SecureLaplaceTest {

    @Test
    public void LaplaceTest() {
        SecureLaplace sl = new SecureLaplace();
        double scale = 10;
        Pair<Integer, Integer> idx = new Pair<>(10, 11);
        double noise = sl.sampleLaplace(idx, scale);
        System.out.println(noise);
    }
}
