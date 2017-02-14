package org.hiero.sketch;

import org.jetbrains.annotations.NotNull;


public class NotNullTest {

    public Integer IntA(int i) {
        if (i > 0) return i;
        else return null;
    }
    
    @NotNull
    public Integer IntB(){
        int j = -10;
        return IntA(j);
    }
}
