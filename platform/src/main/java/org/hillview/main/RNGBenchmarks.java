package org.hillview.main;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.utils.Converters;
import org.hillview.utils.HashUtil;

import java.util.ArrayList;
import java.util.Random;

public class RNGBenchmarks extends Benchmarks {
    static double multiplier = 10.0;
    static double eps = 0.1;
    static double scale = multiplier / eps;
    static Random rand = new Random(10);

    static ArrayList<Pair<Integer, Integer>> generateRandomPairs(int nPairs, int min, int max) {
        ArrayList<Pair<Integer, Integer>> ret = new ArrayList<>();
        for (int i = 0; i < nPairs; i++) {
             ret.add(new Pair(rand.nextInt((max - min) + 1) + min, rand.nextInt((max - min) + 1) + min));
        }

        return ret;
    }

    static void runMurmur(ArrayList<Pair<Integer, Integer>> pairs) {
        LaplaceDistribution dist = new LaplaceDistribution(0, scale);
        int hashCode = 31;
        for (Pair<Integer, Integer> p : pairs) {
            hashCode = HashUtil.murmurHash3(hashCode, p.first);
            hashCode = HashUtil.murmurHash3(hashCode, p.second);
            dist.reseedRandomGenerator(hashCode);
            dist.sample();
        }
    }

    static void runAES(ArrayList<Pair<Integer, Integer>> pairs) {
        SecureLaplace laplace = new SecureLaplace();
        for (Pair<Integer, Integer> p : pairs) {
            laplace.sampleLaplace(p, scale);
        }
    }

    public static void main(String[] args) {
        ArrayList<Pair<Integer, Integer>> pairs = generateRandomPairs(1000000, 100, 100);

        Runnable r = () -> runAES(pairs);
        runNTimes(r, 1, "AES", pairs.size());

        Runnable r2 = () -> runMurmur(pairs);
        runNTimes(r2, 1, "Murmur+CommonsLaplace", pairs.size());
    }
}
