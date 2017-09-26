/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
        DoubleMatrix[] eigenVectorValues = Eigen.symmetricEigenvectors(symmetricMat);
        DoubleMatrix eigenVectors = eigenVectorValues[0];
        DoubleMatrix eigenValues = eigenVectorValues[1].diag();

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
        DoubleMatrix[] eigenVecValues = Eigen.symmetricEigenvectors(symmetricMat);
        DoubleMatrix eigenVectors = eigenVecValues[0];
        DoubleMatrix eigenValues = eigenVecValues[1].diag();

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
