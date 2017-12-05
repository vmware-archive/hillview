/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http:description: www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

interface TestOperation {
    /**
     * Condition which is evaluated to see whether the test can executed.
     */
    cond: () => boolean;
    /**
      * Operation that executes the test.
       */
    cont: () => void;
    description: string;
}

/**
 * Retrieves a node from the DOM starting from a CSS selector specification.
 * @param {string} cssselector  Node specification as a CSS selector.
 * @returns {Node}              The unique selected node.
 */
function findElement(cssselector: string): HTMLElement {
    let val = document.querySelector(cssselector);
    return <HTMLElement>val;
}

/**
 * Creates an event for a context menu.  This is usually triggered by a right mouse-click.
 */
function contextMenuEvent(): Event {
    let evt: MouseEvent = document.createEvent("MouseEvents");
    evt.initMouseEvent('contextmenu', true, true, window, 0, 0, 0, 0, 0, true, false,
        false, false, 2, null);
    return evt;
}

/**
 * Creates a mouse click event with modifiers.
 */
function mouseClickEvent(shift: boolean, control: boolean): Event {
    let evt: MouseEvent = document.createEvent("MouseEvents");
    evt.initMouseEvent("click", true, true, window, 0, 0, 0, 0, 0, control, false, shift, false, 0, null);
    return evt;
}

function keyboardEvent(code: string): Event {
    return new KeyboardEvent("keydown",
        { code: code, altKey: false, bubbles: true, cancelable: true, ctrlKey: false });
}

/**
 * This class is used for testing the UI.
 */
export class Test {
    testProgram: TestOperation[];
    programCounter: number;

    constructor() {
        this.testProgram = [];
        this.programCounter = 0;
    }

    /**
     * Singleton instance of this class.
     */
    public static instance: Test = new Test();

    public addInstruction(testOp: TestOperation): void {
        this.testProgram.push(testOp);
    }

    public addProgram(testOps: TestOperation[]): void {
        this.testProgram = this.testProgram.concat(testOps);
    }

    /**
     * Execute next test if is possible.
     * If the text is executed the next continuation is reset.
     */
    public runNext(): void {
        if (this.testProgram.length <= this.programCounter) {
            console.log("Tests are finished");
            return;
        }
        let op = this.testProgram[this.programCounter];

        if (op.cond()) {
            console.log("Running test " + op.description);
            op.cont();
            this.programCounter++;
        }
    }

    public runTests(): void {
        this.createTestProgram();
        this.runNext();
    }

    public createTestProgram(): void {
        this.addProgram([{
            description: "Load all flights",
            cond: () => true,
            cont: () => findElement("#hillviewPage0 #topMenu #Flights__all_").click()
        }, {
            description: "Show all columns",
            cond: () => findElement("#hillviewPage1 .idle") != null,
            cont: () => findElement("#hillviewPage1 #topMenu #All_columns").click()
        }, {
            description: "Show no columns",
            cond: () => true,
            cont: () => findElement("#hillviewPage1 #topMenu #No_columns").click()
        }, {
            description: "show column 0",
            cond: () => true,
            cont: () => {
                let col0 = findElement("#hillviewPage1 thead .col0");
                let evt = contextMenuEvent();
                col0.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Show").click();
            }
        }, {
            description: "Show column 1",
            cond: () => true,
            cont: () => {
                let col1 = findElement("#hillviewPage1 thead .col1");
                let evt = contextMenuEvent();
                col1.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Show").click();
            }
        }, {
            description: "Hide column 0",
            cond: () => true,
            cont: () => {
                let col0 = findElement("#hillviewPage1 thead .col0");
                let evt = contextMenuEvent();
                col0.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Hide").click();
            }
        }, {
            description: "Show schema view",
            cond: () => true,
            cont: () => {
                findElement("#hillviewPage1 #topMenu #Schema").click();
                // This does not involve an RPC; the result is available right away
                // Select row 0
                let row0 = findElement("#hillviewPage2 #row0");
                row0.click();
                // Add row 1
                let row1 = findElement("#hillviewPage2 #row1");
                let evt = mouseClickEvent(false, true);
                row1.dispatchEvent(evt);  // control-click
                // Add row 3
                let row3 = findElement("#hillviewPage2 #row3");
                row3.dispatchEvent(evt);
                // Select menu item to show the associated table
                findElement("#hillviewPage2 #topMenu #Selected_columns").click();
                // Show a histogram
                let col1 = findElement("#hillviewPage1 thead .col1");
                let revt = contextMenuEvent();
                col1.dispatchEvent(revt);
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            }
        }, {
            description: "Show a categorical histogram",
            cond: () => findElement("#hillviewPage4 .idle") != null,
            cont: () => {
                // Show a histogram
                let col2 = findElement("#hillviewPage1 thead .col2");
                let evt = contextMenuEvent();
                col2.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            }
        }, {
            description: "Show a 2D histogram",
            cond: () => findElement("#hillviewPage5 .idle") != null,
            cont: () => {
                // Show a histogram
                findElement("#hillviewPage1 thead .col8").click();
                let evt = mouseClickEvent(false, true);
                let col9 = findElement("#hillviewPage1 thead .col9");
                col9.dispatchEvent(evt); // control-click
                let revt = contextMenuEvent();
                col9.dispatchEvent(revt);
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            }
        }, {
            description: "Scroll",
            cond: () => findElement("#hillviewPage1 .idle") != null,
            cont: () => {
                let evt = keyboardEvent("PageDown");
                let tableHead = findElement("#hillviewPage1 #tableContainer");
                tableHead.dispatchEvent(evt);
            }
        }, {
            description: "Filter",
            cond: () => findElement("#hillviewPage1 .idle") != null,
            cont: () => {
                // Show a histogram
                let col2 = findElement("#hillviewPage1 thead .col2");
                let evt = contextMenuEvent();
                col2.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Filter___").click();
                (<HTMLInputElement>findElement(".dialog #query")).value = "AA";
                (<HTMLSelectElement>findElement(".dialog #complement")).value = "Equality";
                findElement(".dialog .confirm").click();
            }
        },{
            description: "Close some windows",
            cond: () => true,
            cont: () => {
                /*
                findSelectedElement("#hillviewPage2 .close").click();
                findSelectedElement("#hillviewPage3 .close").click();
                findSelectedElement("#hillviewPage4 .close").click();
                findSelectedElement("#hillviewPage5 .close").click();
                */
            }
        }
        ]);
    }
}
