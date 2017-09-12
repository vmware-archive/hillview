#ifndef _DATA_HPP_
#define _DATA_HPP_

#include <cassert>

class DoubleColumn {
    int size;
    double* data;

 public:
    DoubleColumn(int size): size(size) {
        assert(size > 0);
        data = new double[size];
    }

    void fillRandom(double min, double max) {
        for (int i=0; i < this->size; i++)
            this->data[i] = min + i % (int)(max - min);
    }

    void histogram(Histogram* histo) {
        for (int i=0; i < this->size; i++)
            histo->add(this->data[i]);
    }

    void print(std::ostream& out) const {
        for (int i=0; i < this->size; i++) {
            if (this->data[i] != 0)
                out << "[" << i << "]=" << this->data[i] << " ";
        }
    }
};

#endif /* _DATA_HPP_ */
