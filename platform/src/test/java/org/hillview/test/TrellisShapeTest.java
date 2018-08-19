package org.hillview.test;


import org.junit.Test;

public class TrellisShapeTest {

    @Test
    public void tShapeTest() {
        double[] ratios = {1.25, 1.5, 2};
        for (double ratio : ratios) {
            ComputeTrellisShape tShape = new ComputeTrellisShape(70, 10, 40, 10, ratio);
            System.out.printf("\nAspect ratio %f\n", ratio);
            for (int n = 10; n <= 30; n++) {
                ComputeTrellisShape.TrellisShape optShape = tShape.getShape(n);
                if (optShape.x_num * optShape.y_num < n)
                    System.out.printf("Constraints are not feasible for ");
                System.out.printf("%d: %d * %d, Size %.0f * %.0f, Coverage: %f \n", n, optShape.x_num,
                        optShape.y_num, optShape.x_len, optShape.y_len, optShape.coverage);
            }
        }
    }
}