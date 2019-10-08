/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.RpcTarget;
import org.hillview.dataset.api.IJson;
import org.hillview.sketches.results.CorrMatrix;
import org.jblas.DoubleMatrix;

import java.util.Arrays;

import static org.jblas.Eigen.symmetricEigenvalues;

final class CorrelationMatrixTarget extends RpcTarget {
    final CorrMatrix corrMatrix;

    /**
     * This class contains a pointer to the Correlation matrix, and the top singular values, along
     * with some metadata about them.
     */
    public static class EigenVal implements IJson {
        /**
         * The top few singular values in descending order. The cutoff is (top SV)/100.
         */
        final double [] eigenValues;
        /**
         * The variance accounted for by the above singular values.
         */
        double explainedVar = 0;
        /**
         * The total variance.
         */
        double totalVar = 0;
        /**
         * Id of the correlation matrix. Can be used for performing PCA on the data.
         */
        final RpcTarget.Id correlationMatrixTargetId;

        EigenVal(double[] data, RpcTarget.Id matrixId) {
            this.correlationMatrixTargetId = matrixId;
            double cutOff = data[data.length - 1]* 0.01;
            int numLarge = 0;
            for (int i = 0; (i < data.length); i++) {
                double thisEigen = data[data.length - 1 - i];
                if (thisEigen >=  cutOff) {
                    numLarge++;
                    explainedVar += thisEigen;
                }
                totalVar += thisEigen;
            }
            this.eigenValues = new double[numLarge];
            for (int i = 0; i < numLarge; i++)
                this.eigenValues[i] = data[data.length -i -1];
        }

        public String toString() {
            return Arrays.toString(this.eigenValues) + "\n Total Variance: "+ totalVar +
                    "\n Explained Variance: "+ explainedVar;
        }
    }


    CorrelationMatrixTarget(final CorrMatrix cm, HillviewComputation computation) {
        super(computation);
        this.corrMatrix = cm;
        this.registerObject();
    }

    public EigenVal eigenValues() {
        DoubleMatrix dm = symmetricEigenvalues(
                new DoubleMatrix(this.corrMatrix.getCorrelationMatrix()));
        return new EigenVal(dm.data, this.getId());
    }
}
