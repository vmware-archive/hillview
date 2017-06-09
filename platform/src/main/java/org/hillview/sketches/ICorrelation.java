package org.hillview.sketches;

/**
 * An interface for classes that compute inner products, correlations and related statistics between
 * columns in a table.
 */
public interface ICorrelation {

    /**
     * Computes the normalized L_2 of a column.
     * @param s = {s_1,...,s_n} a column of double/integer values.
     * @return Norm(s) = Sqrt((\sum_i s_i^2)/n), or equivalently the RMS value.
     */
    double getNorm(String s);

    /**
     * Computes the normalized inner product between two columns.
     * @param s = {s_1,...,s_n} a column of double/integer values.
     * @param t = {t_1,...,t_n} a column of double/integer values
     * @return IP(s,t) = (\sum_i x_i*y_i)/n.
     */
    double getInnerProduct(String s, String t);

    /**
     * The correlation can be thought of as the cosine of the angle between the two columns viewed
     * as real vectors. It always lies in the range [-1,1].
     * @param s = {s_1,...,s_n} a column of double/integer values.
     * @param t = {t_1,...,t_n} a column of double/integer values
     * @return Corr(s,t) = (\sum_i x_i*y_i)/Norm(x)*Norm(y).
     */
    double getCorrelation(String s, String t);

    /**
     * Returns the vector of correlations of all columns with a given column.
     * @param s The given column.
     * @return The vector of correlations of all columns with s.
     */
    double[] getCorrelationWith(String s);

    /**
     * Returns the matrix of pairwise correlations between every pair of columns.
     * @return A matrix of doubles whose (i,j)-th entry is the correlation between column i
     * and column j.
     */
    double[][] getCorrelationMatrix();
}
