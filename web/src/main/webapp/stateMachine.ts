/**
 * This class stores a subset of states from some universe.
 * It supports membership queries, as well as various kind of state transitions.
 */
export class stateMachine {
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
     * @param {number} type: takes on 3 values, depending on which key if any is pressed. 0 means no key,
     * 1 means Ctrl/Esc, and 2 means Shift.
     * @param {number} val: this is a numeric value from the universe of values, of which we are storing a subset.
     * It indicates which row/column is currently clicked.
     * The semantics are as follows:
     * - Type 0. Toggle the membership of val. Delete everything else.
     * - Type 1. Toggle the membership of val. The rest stays unchanged.
     * - Type 2. Toggle the state of val. Change the state of the open interval from the last clicked state to val,
     * so that its state matches that of val.
     */
    public changeState(type: number, val: number) {
        if (type == 0) { //No buttons pressed, forget everything else, toggle val
            let isPresent: boolean = this.has(val);
            this.selected.clear();
            if (!isPresent)
                this.selected.add(val);
            this.curState = val;
        }
        else if (type == 1) { //Ctrl or Esc pressed, keep everything else, toggle val
            this.toggle(val);
            this.curState = val;
        }
        else if (type == 2) { //Shift pressed, toggle states in the open interval
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