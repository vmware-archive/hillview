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
            if (val > this.curState) {
                for (let i = this.curState + 1; i <= val; i++)
                    this.toggle(i);
            } else if (val < this.curState) {
                for (let i = val; i < this.curState; i++)
                    this.toggle(i);
            }
            this.curState = val;
        }
    }
}