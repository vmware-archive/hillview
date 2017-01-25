export interface IHtmlElement {
    getHTMLRepresentation() : HTMLElement;
}

export class ScrollBar implements IHtmlElement {
    // Only vertical scroll bars supported
    // Range for start and end is 0-1
    start : number;
    end : number;

    private outer  : HTMLElement;
    private inner  : HTMLElement;
    private before : HTMLElement;
    private after  : HTMLElement;

    constructor() {
        this.outer = document.createElement("div");
        this.outer.className = "scrollbarOuter";

        this.inner = document.createElement("div");
        this.inner.className = "scrollBarInner";

        this.before = document.createElement("div");
        this.before.className = "scrollBarBefore";

        this.after = document.createElement("div");
        this.after.className = "scrollBarAfter";

        this.outer.appendChild(this.before);
        this.outer.appendChild(this.inner);
        this.outer.appendChild(this.after);
        this.setPosition(0, 1);
    }

    getHTMLRepresentation() : HTMLElement {
        return this.outer;
    }

    computePosition() : void {
        if (this.start <= 0.0 && this.end >= 1.0)
            this.outer.style.visibility = 'hidden';
        else
            this.outer.style.visibility = 'visible';
        this.before.style.height = String(this.start * 100) + "%";
        this.inner.style.height = String((this.end - this.start) * 100) + "%";
        this.after.style.height = String((1 - this.end) * 100) + "%";
    }

    setPosition(start : number, end: number) : void {
        if (start > end)
            throw "Start after end: " + start + "/" + end;
        this.start = start;
        this.end = end;
        this.computePosition();
    }
}

export class ProgressBar implements IHtmlElement {
    end: number;

    private outer : HTMLElement;
    private bar   : HTMLElement;

    constructor() {
        this.outer = document.createElement("div");
        this.outer.className = "progressBarOuter";

        this.bar = document.createElement("div");
        this.bar.className = "progressBarInner";

        this.outer.appendChild(this.bar);
        this.setPosition(1.0);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }

    setPosition(end: number) : void {
        if (end < 0)
            end = 0;
        if (end > 1)
            end = 1;
        this.end = end;
        this.computePosition();
    }

    computePosition() : void {
        this.bar.style.width = String(this.end * 100) + "%";
    }
}