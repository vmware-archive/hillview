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
function findSelectedElement(cssselector: string): HTMLElement {
    let val = document.querySelector(cssselector);
    return <HTMLElement>val;
}

/**
 * Creates an event for a context menu.  This is usually triggered by a right mouse-click.
 */
function contextMenuEvent(): Event {
    let evt = document.createEvent("HTMLEvents");
    evt.initEvent('contextmenu', true, true); // bubbles = true, cancelable = true
    return evt;
}

/**
 * Creates a mouse click event with modifiers.
 */
function mouseClick(shift: boolean, control: boolean): Event {
    let evt = document.createEvent("MouseEvents");
    evt.initMouseEvent("click", true, true, window, 0, 0, 0, 0, 0, control, false, shift, false, 0, null);
    return evt;
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
            cont: () => findSelectedElement("#hillviewPage0 #topMenu #Flights__all_").click()
        }, {
            description: "Show all columns",
            cond: () => findSelectedElement("#hillviewPage1") != null,
            cont: () => findSelectedElement("#hillviewPage1 #topMenu #All_columns").click()
        }, {
            description: "Show no columns",
            cond: () => true,
            cont: () => findSelectedElement("#hillviewPage1 #topMenu #No_columns").click()
        }, {
            description: "show column 0",
            cond: () => true,
            cont: () => {
                let col0 = findSelectedElement("#hillviewPage1 thead .col0");
                let evt = contextMenuEvent();
                col0.dispatchEvent(evt);
                findSelectedElement("#hillviewPage1 .dropdown #Show").click();
            }
        }, {
            description: "Show column 1",
            cond: () => true,
            cont: () => {
                let col1 = findSelectedElement("#hillviewPage1 thead .col1");
                let evt = contextMenuEvent();
                col1.dispatchEvent(evt);
                findSelectedElement("#hillviewPage1 .dropdown #Show").click();
            }
        }, {
            description: "Hide column 0",
            cond: () => true,
            cont: () => {
                let col0 = findSelectedElement("#hillviewPage1 thead .col0");
                let evt = contextMenuEvent();
                col0.dispatchEvent(evt);
                findSelectedElement("#hillviewPage1 .dropdown #Hide").click();
            }
        }, {
            description: "Show schema view",
            cond: () => true,
            cont: () => {
                findSelectedElement("#hillviewPage1 #topMenu #Schema").click();
                // This does not involve an RPC; the result is available right away
                // Select row 0
                let row0 = findSelectedElement("#hillviewPage2 #row0");
                row0.click();
                // Add row 1
                let row1 = findSelectedElement("#hillviewPage2 #row1");
                let evt = mouseClick(false, true);
                row1.dispatchEvent(evt);
                // Add row 3
                let row3 = findSelectedElement("#hillviewPage2 #row3");
                row3.dispatchEvent(evt);
                // Select menu item to show the associated table
                findSelectedElement("#hillviewPage2 #topMenu #Selected_columns").click();
                // This does not involve an RPC, it finished right away
                // Close schema view
                findSelectedElement("#hillviewPage2 .close").click()
                // Close table view
                findSelectedElement("#hillviewPage3 .close").click()
                // Show a histogram
                let col1 = findSelectedElement("#hillviewPage1 thead .col1");
                let revt = contextMenuEvent();
                col1.dispatchEvent(revt);
                findSelectedElement("#hillviewPage1 .dropdown #Histogram").click();
            }
        }]);
    }
}
