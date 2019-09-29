/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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
 */

package org.hillview.test.dataStructures;


import org.hillview.test.BaseTest;
import org.hillview.test.dataStructures.ComputeTrellisShape;
import org.junit.Test;

public class TrellisShapeTest extends BaseTest {
    @Test
    public void tShapeTest() {
        double[] ratios = {1.25, 1.5, 2};
        for (double ratio : ratios) {
            ComputeTrellisShape tShape = new ComputeTrellisShape(70, 10, 50,
                    10, ratio, 5);
            if (toPrint)
                System.out.printf("\nAspect ratio %f\n", ratio);
            for (int n = 10; n <= 25; n++) {
                ComputeTrellisShape.TrellisShape optShape = tShape.getShape(n);
                if (toPrint) {
                    if (optShape.x_num * optShape.y_num < n)
                        System.out.println("Constraints are not feasible for ");
                    System.out.printf("%d: %d * %d, Size %.0f * %.0f, Coverage: %f \n", n,
                            optShape.x_num, optShape.y_num, optShape.x_len, optShape.y_len,
                            optShape.coverage);
                }
            }
        }
    }
}
