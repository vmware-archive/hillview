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

type TransitionType = "NoKey" | "Ctrl" | "Shift";

/**
 * This class stores a subset of states from a universe of integers.
 * It supports membership queries, as well as various kind of state transitions.
 * This is used to model how selection of multiple objects arranged in a row is handled
 * for various combinations of operations (e.g., column selection).
 */
export class SelectionStateMachine {
    protected selected: Set<number>;
    protected curState: number;

    constructor() {
        this.selected = new Set<number>();
        this.curState = null;
    }

    public size(): number {
        return this.selected.size;
    }

    public getStates(): Set<number> {
        return this.selected;
    }

    public has(val: number): boolean{
        return this.selected.has(val);
    }

    public clear() {
        this.selected.clear();
        this.curState = null;
    }

    private toggle(val: number) {
        if(this.selected.has(val))
            this.selected.delete(val);
        else
            this.selected.add(val);
    }

    public add(val: number) {
        this.selected.add(val);
    }

    public delete(val: number) {
        this.selected.delete(val);
    }


    /**
     * Changes the membership for all states in the interval [a,b] to the boolean value to.
     * Is used when the shift key is pressed.
     */
    private rangeChange(a: number, b: number, to: boolean) {
        for (let i = a; i <= b; i++) {
            if (to)
                this.selected.add(i);
            else
                this.selected.delete(i);
        }
    }

    /**
     * This method changes the set of selected states, given a numeric value, and type of transition.
     * @param {TransitionType} type: Specifies which keybaord key was pressed. Currently takes on 3 values.
     * @param {number} val: this is a numeric value from the universe of values, of which we are storing a subset.
     * It indicates which row/column is currently clicked.
     * The semantics are as follows:
     * - Type NoKey: Toggle the membership of val. Delete everything else.
     * - Type Ctrl: Toggle the membership of val. The rest stays unchanged.
     * - Type Shift: Toggle the state of val. Change the state of the open interval from the last clicked state to val,
     * so that its state matches that of val.
     */
    public changeState(type: TransitionType, val: number) {
        if (type == "NoKey") { //No buttons pressed, forget everything else, toggle val
            let isPresent: boolean = this.has(val);
            this.selected.clear();
            if (!isPresent)
                this.selected.add(val);
            this.curState = val;
        }
        else if (type == "Ctrl") { //Ctrl or Esc pressed, keep everything else, toggle val
            this.toggle(val);
            this.curState = val;
        }
        else if (type == "Shift") { //Shift pressed, toggle states in the open interval
            // curState to val
            let current: boolean = this.selected.has(val);
            if (val > this.curState)
                this.rangeChange(this.curState + 1, val, !current);
            else if (val < this.curState)
                this.rangeChange(val, this.curState -1, !current);
            this.curState = val;
        }
    }
}