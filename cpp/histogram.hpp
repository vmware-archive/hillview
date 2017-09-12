#ifndef _HISTOGRAM_HPP_
#define _HISTOGRAM_HPP_

#include <cassert>
#include <ostream>

class Histogram {
    int *buckets;
    int bucketCount;
    double min;
    double max;
    double range;
    int outOfBounds;
    int missing;

 public:
    Histogram(int bucketCount, double min, double max):
            bucketCount(bucketCount), min(min), max(max), outOfBounds(0), missing(0), range(max - min) {
        assert(bucketCount > 0);
        assert(max >= min);
        assert(range >= 0);
        if (max == min)
            assert(bucketCount == 1);
        this->buckets = new int[bucketCount];
        for (int i=0; i < this->bucketCount; i++)
            this->buckets[i] = 0;
    }

    inline void add(double value) {
        int bucketIndex;
        if (value < this->min || value > this->max) {
            this->outOfBounds++;
            return;
        }
        if (range <= 0)
            bucketIndex = 0;
        else
            bucketIndex = (int)(this->bucketCount * (value - this->min) / (this->max - this->min));
        assert(bucketIndex >= 0);
        assert(bucketIndex < this->bucketCount);
        this->buckets[bucketIndex]++;
    }

    void addMissing() {
        this->missing++;
    }

    void print(std::ostream& out) const {
        for (int i=0; i < this->bucketCount; i++)
            out << "[" << i << "]=" << this->buckets[i] << " ";
        out << std::endl;
    }
};

#endif /* _HISTOGRAM_HPP_ */
