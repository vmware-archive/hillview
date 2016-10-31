package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.*;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;

import java.util.ArrayList;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

public class DatasetTest {
    private class Increment implements IMap<Integer, Integer> {
        @Override
        public Observable<PartialResult<Integer>> apply(final Integer data) {
            return Observable.just(new PartialResult<Integer>(1.0, data + 1));
        }
    }

    private class Sketch implements ISketch<Integer, Integer> {
        @Override
        public Integer zero() {
            return 0;
        }

        @Override
        public Integer add(final Integer left, final Integer right) {
            return left + right;
        }

        @Override
        public Observable<PartialResult<Integer>> create(final Integer data) {
            return Observable.just(new PartialResult<Integer>(1.0, data));
        }
    }

    @Test
    public void localDataSetTest() {
        final LocalDataSet<Integer> ld = new LocalDataSet<Integer>(4);
        final Increment increment = new Increment();

        final IDataSet<Integer> r = ld.blockingMap(increment);
        final Sketch sketch = new Sketch();
        final int result = r.blockingSketch(sketch);
        assertEquals(result, 5);
    }

    @Test
    public void parallelDataSetTest() {
        final LocalDataSet<Integer> ld1 = new LocalDataSet<Integer>(4);
        final LocalDataSet<Integer> ld2 = new LocalDataSet<Integer>(5);
        final ArrayList<IDataSet<Integer>> elems = new ArrayList<IDataSet<Integer>>(2);
        elems.add(ld1);
        elems.add(ld2);
        final ParallelDataSet<Integer> par = new ParallelDataSet<>(elems);
        final Increment increment = new Increment();

        final IDataSet<Integer> r1 = par.blockingMap(increment);
        final Sketch sketch = new Sketch();
        final int result = r1.blockingSketch(sketch);
        assertEquals(result, 11);

        final IDataSet<Integer> r2 = r1.blockingMap(increment);
        final int result1 = r2.blockingSketch(sketch);
        assertEquals(result1, 13);
    }

    private class Sum implements ISketch<int[], Integer> {
        @Override
        public Integer zero() {
            return 0;
        }

        @Override
        public Integer add(final Integer left, final Integer right) {
            return left + right;
        }

        @Override
        public Observable<PartialResult<Integer>> create(final int[] data) {
            final int parts = 10;
            return Observable.range(0, parts).map(index -> {
                final int partSize = data.length / parts;
                final int left = partSize * index;
                final int right = (index == (parts - 1)) ? data.length : (left + partSize);
                int sum1 = 0;
                for (int i=left; i < right; i++)
                    sum1 += data[i];
                return new PartialResult<Integer>(1.0 / parts, sum1);
            });
        }
    }

    private final int largeSize = 10 * 1024 * 1024;

    private IDataSet<int[]> createLargeDataset() {
        final int[] data = new int[this.largeSize];
        for (int i=0; i < this.largeSize; i++)
            data[i] = ((i % 10) == 0) ? 0 : i;
        return new LocalDataSet<int[]>(data);
    }

    @Test
    public void largeDataSetTest() {
        final IDataSet<int[]> ld = this.createLargeDataset();
        final int result = ld.blockingSketch(new Sum());
        int sum = 0;
        for (int i=0; i < this.largeSize; i++)
            sum += ((i % 10) == 0) ? 0 : i;
        assertEquals(result, sum);
    }

    @Test
    public void unsubscriptionTest() {
        final IDataSet<int[]> ld = this.createLargeDataset();
        final Observable<PartialResult<Integer>> pr = ld.sketch(new Sum());
        pr.subscribe(new Subscriber<PartialResult<Integer>>() {
            private int count = 0;
            private double done = 0.0;

            @Override
            public void onCompleted() {
                fail("Unreachable");
            }

            @Override
            public void onError(final Throwable throwable) {
                fail("Unreachable");
            }

            @Override
            public void onNext(final PartialResult<Integer> pr) {
                done += pr.deltaDone;
                this.count++;
                if (this.count == 3)
                    this.unsubscribe();
                else
                    assertEquals(done, 0.1 * this.count);
            }
        });
    }
}
