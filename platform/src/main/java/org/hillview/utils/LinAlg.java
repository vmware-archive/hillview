package org.hillview.utils;

import org.jblas.DoubleMatrix;
import org.jblas.Eigen;
import org.jblas.ranges.AllRange;
import org.jblas.ranges.IndicesRange;

public class LinAlg {
    /**
     * @param symmetricMat The (symmetric!) matrix of which the eigenvalues have to be computed.
     * @param n The number of eigenvectors to return.
     * @return A DoubleMatrix whose *rows* are the n eigenvectors of symmetricMat corresponding to the n largest eigenvalues
     * of symmetricMat.
     */
    public static DoubleMatrix eigenVectors(DoubleMatrix symmetricMat, int n) {
        // Compute eigen{vectors/values} and unpack the result.
        DoubleMatrix[] eigenVecVals = Eigen.symmetricEigenvectors(symmetricMat);
        DoubleMatrix eigenVectors = eigenVecVals[0];
        DoubleMatrix eigenValues = eigenVecVals[1].diag();

        // Sort them by the eigenvalues and get the indices of the largest n.
        int[] order = eigenValues.sortingPermutation();
        int[] largestN = new int[n];
        for (int i = 0; i < n; i++) {
            largestN[i] = order[order.length - i - 1];
        }

        // Slice the eigenvector matrix to get the result (it has the vectors as *columns*, we want them as *rows*).
        return eigenVectors.get(new AllRange(), new IndicesRange(largestN)).transpose();
    }

    /**
     * @param symmetricMat The (symmetric!) matrix of which the eigenvalues have to be computed.
     * @param n The number of eigenvectors to return.
     * @return An array of DoubleMatrices. The first one is the matrix whose rows are the n eigenvectors of
     * symmetricMat. The second matrix is a column vector that contains the fraction of the variance explained by the
     * eigenvectors, in the same order.
     */
    public static DoubleMatrix[] eigenVectorsVarianceExplained(DoubleMatrix symmetricMat, int n) {
        // Compute eigen{vectors/values} and unpack the result.
        DoubleMatrix[] eigenVecVals = Eigen.symmetricEigenvectors(symmetricMat);
        DoubleMatrix eigenVectors = eigenVecVals[0];
        DoubleMatrix eigenValues = eigenVecVals[1].diag();

        // Sort them by the eigenvalues and get the indices of the largest n.
        int[] order = eigenValues.sortingPermutation();
        int[] largestN = new int[n];
        for (int i = 0; i < n; i++) {
            largestN[i] = order[order.length - i - 1];
        }

        // Slice the eigenvector matrix to get the result (it has the vectors as *columns*, we want them as *rows*).
        return new DoubleMatrix[]{
                eigenVectors.get(new AllRange(), new IndicesRange(largestN)).transpose(),
                eigenValues.get(new IndicesRange(largestN), 0).div(eigenValues.sum())
        };
    }
}
