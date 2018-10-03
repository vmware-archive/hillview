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

import {NotifyDialog} from "./ui/dialog";
import {findElement} from "./util";

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
 * Creates an event for a context menu.  This is usually triggered by a right mouse-click.
 */
function contextMenuEvent(): Event {
    const evt: MouseEvent = document.createEvent("MouseEvents");
    evt.initMouseEvent("contextmenu", true, true, window, 0, 0, 0, 0, 0, true, false,
        false, false, 2, null);
    return evt;
}

/**
 * Creates a mouse click event with modifiers.
 */
function mouseClickEvent(shift: boolean, control: boolean): Event {
    const evt: MouseEvent = document.createEvent("MouseEvents");
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
    protected testProgram: TestOperation[];
    protected programCounter: number;

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
            if (this.testProgram.length !== 0)
                console.log("Tests are finished");
            return;
        }
        const op = this.testProgram[this.programCounter];
        if (op == null)
            return;

        if (op.cond()) {
            console.log("Running test " + op.description);
            op.cont();
            this.programCounter++;
        }
    }

    private next(): void {
        this.programCounter++;
        this.runNext();
    }

    public runTests(): void {
        this.createTestProgram();
        this.runNext();
    }

    public createTestProgram(): void {
        this.addProgram([{
            description: "Load all flights",
            cond: () => true,
            cont: () => findElement("#hillviewPage0 .topMenu #Flights__15_columns__CSV_").click(),
        }, /* {
        This menu has been disabled.
            description: "Show all columns",
            cond: () => findElement("#hillviewPage1 .idle") != null,
            cont: () => findElement("#hillviewPage1 .topMenu #All_columns").click()
        }, */ {
            description: "Show no columns",
            cond: () => findElement("#hillviewPage1 .idle") != null,
            cont: () => findElement("#hillviewPage1 .topMenu #No_columns").click(),
        }, {
            description: "show column 0",
            cond: () => true,
            cont: () => {
                const col0 = findElement("#hillviewPage1 thead .col0");
                const evt = contextMenuEvent();
                col0.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Show").click();
            },
        }, {
            description: "Show column 1",
            cond: () => true,
            cont: () => {
                const col1 = findElement("#hillviewPage1 thead .col1");
                const evt = contextMenuEvent();
                col1.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Show").click();
            },
        }, {
            description: "Hide column 0",
            cond: () => true,
            cont: () => {
                const col0 = findElement("#hillviewPage1 thead .col0");
                const evt = contextMenuEvent();
                col0.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Hide").click();
            },
        }, {
            description: "Show schema view",
            cond: () => true,
            cont: () => {
                findElement("#hillviewPage1 .topMenu #View #Schema").click();
                // This does not involve an RPC; the result is available right away
                // Select row 0
                const row0 = findElement("#hillviewPage2 #row0");
                row0.click();
                // Add row 1
                const row1 = findElement("#hillviewPage2 #row1");
                const evt = mouseClickEvent(false, true);
                row1.dispatchEvent(evt);  // control-click
                // Add row 3
                const row3 = findElement("#hillviewPage2 #row3");
                row3.dispatchEvent(evt);
                // Select menu item to show the associated table
                findElement("#hillviewPage2 .topMenu #Selected_columns").click();
                // Show a histogram
                const col1 = findElement("#hillviewPage1 thead .col1");
                const revt = contextMenuEvent();
                col1.dispatchEvent(revt);
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            },
        }, {
            description: "Show a categorical histogram",
            cond: () => findElement("#hillviewPage4 .idle") != null,
            cont: () => {
                // Show a histogram
                const col2 = findElement("#hillviewPage1 thead .col2");
                const evt = contextMenuEvent();
                col2.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            },
        }, {
            description: "Show a 2D histogram",
            cond: () => findElement("#hillviewPage5 .idle") != null,
            cont: () => {
                // Show a histogram
                findElement("#hillviewPage1 thead .col8").click();
                const evt = mouseClickEvent(false, true);
                const col9 = findElement("#hillviewPage1 thead .col9");
                col9.dispatchEvent(evt); // control-click
                const revt = contextMenuEvent();
                col9.dispatchEvent(revt);
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            },
        }, {
            description: "Scroll",
            cond: () => findElement("#hillviewPage1 .idle") != null,
            cont: () => {
                const evt = keyboardEvent("PageDown");
                const tableHead = findElement("#hillviewPage1 #tableContainer");
                tableHead.dispatchEvent(evt);
            },
        }, {
            description: "Filter",
            cond: () => findElement("#hillviewPage1 .idle") != null,
            cont: () => {
                // Show a histogram
                const col2 = findElement("#hillviewPage1 thead .col2");
                const evt = contextMenuEvent();
                col2.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Filter___").click();
                (findElement(".dialog #query") as HTMLInputElement).value = "AA";
                findElement(".dialog .confirm").click();
            },
        }, {
            description: "Change buckets",
            cond: () => findElement("#hillviewPage7 .idle") != null,
            cont: () => {
                const el2 = findElement("#hillviewPage6 .topMenu #__buckets___");
                el2.click();
                (findElement(".dialog #n_buckets") as HTMLInputElement).value = "10";
                findElement(".dialog .confirm").click();
                this.next(); // changing buckets does not involve an RPC
            },
        }, {
            description: "Close some windows",
            cond: () => true,
            cont: () => {
                for (let i = 2; i < 8; i++) {
                    const el = findElement("#hillviewPage" + i.toString() + " .close");
                    el.click();
                }
                const dialog = new NotifyDialog("Tests are completed", null, "Done.");
                dialog.show();
            },
        },
        ]);
    }
}
