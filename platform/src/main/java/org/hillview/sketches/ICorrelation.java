/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
