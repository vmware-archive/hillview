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
import {assert, findElement} from "./util";

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
    evt.initMouseEvent("contextmenu",
        /* canBubble */ true,
        /* cancellable */ true,
        window,
        /* detail */ 0,
        /* screenX */ 0,
        /* screenY */ 0,
        /* clientX */ 0,
        /* clientY */ 0,
        /* ctrlKey */ false,
        /* altKey */ false,
        /* shiftKey */ false,
        /* metaKey */ false,
        /* button */ 2,
        /* relatedTarget */ null);
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

function controlClickEvent(): Event {
    return mouseClickEvent(false, true);
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

        console.log("Evaluating condition for " + this.programCounter + ". " + op.description);
        if (op.cond()) {
            console.log("Running test " + this.programCounter + ". " + op.description);
            op.cont();
            console.log("Test completed " + this.programCounter + ". " + op.description);
            this.programCounter++;
            console.log("Incremented: " + this.programCounter);
        }
    }

    private next(): void {
        // Trigger a request to the remote site using a "ping" request
        findElement("#hillviewPage0 .topMenu #Manage #List_machines").click();
    }

    public runTests(): void {
        this.createTestProgram();
        this.programCounter = 0;
        this.runNext();
    }
    
    private static existsElement(cssselector: string): boolean {
        const result = findElement(cssselector, true) != null;
        console.log("Checking element existence: " + cssselector + "=" + result);
        return result;
    }

    public createTestProgram(): void {
        /*
         This produces the following pages:
            1: a tabular view
            2: schema view
            3: table view with 3 columns
            4: Histogram of the FlightDate column
            5: Histogram of UniqueCarrier, shown as pie chart
            6: 2dHistogram of DepTime, Depdelay
            7: Table view, filtered flights
            8: Trellis 2D histograms (DepTime, DepDelay) grouped by ActualElapsedTime
            9: Trellis Histograms of UniqueCarrier grouped by ActualElapsedTime
            10: Trellis heatmap plot
            11: Quartiles vector plot
            12: Non-stacked bar charts plot
         */
        this.addProgram([{
            description: "Load all flights",
            cond: () => true,
            cont: () => findElement("#hillviewPage0 .topMenu #Flights__15_columns__CSV_").click(),
        }, {
            description: "Show no columns",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => findElement("#hillviewPage1 .topMenu #No_columns").click(),
        }, {
            description: "rename column FlightDate to Date",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                const date = findElement("#hillviewPage1 thead td[data-colname=FlightDate] .truncated");
                const evt = contextMenuEvent();
                date.dispatchEvent(evt);
                const rename = findElement("#hillviewPage1 .dropdown #Rename___");
                rename.click();
                const formField = findElement(".dialog #name");
                (formField as HTMLInputElement).value = "Date";
                const confirm = findElement(".dialog .confirm");
                confirm.click();
                this.next(); // synchronous -- fall into next test
            }
        }, {
            description: "drop column Cancelled",
            cond: () => true,
            cont: () => {
                // Drop column cancelled
                const cancCol = findElement("#hillviewPage1 thead td[data-colname=Cancelled] .truncated");
                const evt = contextMenuEvent();
                cancCol.dispatchEvent(evt);
                const item = findElement("#hillviewPage1 .dropdown #Drop");
                item.click();
            },
        }, {
            description: "show column 0",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                // Check that Cancelled column does not exist
                assert(!Test.existsElement("#hillviewPage1 thead td[data-colname=Cancelled]"));
                const col0 = findElement("#hillviewPage1 thead .col0");
                const evt = contextMenuEvent();
                col0.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Show").click();
            },
        }, {
            description: "Show column 1",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                const col1 = findElement("#hillviewPage1 thead .col1");
                const evt = contextMenuEvent();
                col1.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Show").click();
            },
        }, {
            description: "Hide column 0",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                const col0 = findElement("#hillviewPage1 thead .col0");
                const evt = contextMenuEvent();
                col0.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Hide").click();
            },
        }, {
            description: "Create column in JavaScript",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                const cancCol = findElement("#hillviewPage1 thead td[data-colname=OriginCityName] .truncated");
                const evt = contextMenuEvent();
                cancCol.dispatchEvent(evt);
                const item = findElement("#hillviewPage1 .dropdown #Create_column_in_JS___");
                item.click();
                (findElement(".dialog #outColName") as HTMLInputElement).value = "O";
                (findElement(".dialog #outColKind") as HTMLInputElement).value = "String";
                (findElement(".dialog #function") as HTMLInputElement).value = "return row['OriginCityName'][0];";
                findElement(".dialog .confirm").click();
            },
        }, {
            description: "Show schema view",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                findElement("#hillviewPage1 .topMenu #View #Schema").click();
                // This does not involve an RPC; the result is available right away.
                // Produces hillviewPage2
                // Select row 0
                const row0 = findElement("#hillviewPage2 #row0");
                row0.click();
                // Add row 1
                const row1 = findElement("#hillviewPage2 #row1");
                const evt = controlClickEvent();
                row1.dispatchEvent(evt);
                // Add row 3
                const row3 = findElement("#hillviewPage2 #row3");
                row3.dispatchEvent(evt);
                // Select menu item to show the associated table
                findElement("#hillviewPage2 .topMenu #Selected_columns").click();
                this.next(); // no rpc
            }
        }, {
            description: "Show histogram from schema view",
            cond: () => true,
            cont: () => {
                const col1 = findElement("#hillviewPage1 thead .col1");
                const revt = contextMenuEvent();
                col1.dispatchEvent(revt);
                // Produces hillviewPage4
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            },
        }, {
            description: "Show a categorical histogram",
            cond: () => Test.existsElement("#hillviewPage4 .idle"),
            cont: () => {
                // Show a histogram
                const col2 = findElement("#hillviewPage1 thead .col2");
                const evt = contextMenuEvent();
                col2.dispatchEvent(evt);
                // Produces hillviewPage5
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            },
        }, {
            description: "Show a pie chart",
            cond: () => Test.existsElement("#hillviewPage5 .idle"),
            cont: () => {
                // Show a histogram
                const pie = findElement("#hillviewPage5 .topMenu #View #pie_chart_histogram");
                pie.click();
                this.next();
            },
        }, {
            description: "Show a 2D histogram",
            cond: () => Test.existsElement("#hillviewPage5 .idle"),
            cont: () => {
                // Show a histogram
                findElement("#hillviewPage1 thead .col8").click();
                const evt = controlClickEvent();
                const col9 = findElement("#hillviewPage1 thead .col9");
                col9.dispatchEvent(evt);
                const revt = contextMenuEvent();
                col9.dispatchEvent(revt);
                // Produces hillviewPage6
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            },
        }, {
            description: "Scroll",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                const evt = keyboardEvent("PageDown");
                const tableHead = findElement("#hillviewPage1 #tableContainer");
                tableHead.dispatchEvent(evt);
            },
        }, {
            description: "Filter",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                // Show a histogram
                const col2 = findElement("#hillviewPage1 thead .col2");
                const evt = contextMenuEvent();
                col2.dispatchEvent(evt);
                findElement("#hillviewPage1 .dropdown #Filter___").click();
                (findElement(".dialog #query") as HTMLInputElement).value = "AA";
                // Produces hillviewPage7
                findElement(".dialog .confirm").click();
            },
        }, {
           description: "Trellis histogram plot",
           cond: () => Test.existsElement("#hillviewPage6 .idle"),
           cont: () => {
               findElement("#hillviewPage6 .topMenu #View #group_by___").click();
               // Produces hillviewPage8
               (findElement(".dialog .confirm")).click();
           }
        }, {
            description: "Trellis 2d histogram plot",
            cond: () => Test.existsElement("#hillviewPage8 .idle"),
            cont: () => {
                findElement("#hillviewPage5 .topMenu #View #group_by___").click();
                // Produces hillviewPage9
                (findElement(".dialog .confirm")).click();
            }
        }, {
            description: "Trellis heatmap plot",
            cond: () => Test.existsElement("#hillviewPage9 .idle"),
            cont: () => {
                // Produces hillviewPage10
                findElement("#hillviewPage8 .topMenu #View #heatmap").click();
            }
        }, {
            description: "Change buckets",
            cond: () => Test.existsElement("#hillviewPage10 .idle"),
            cont: () => {
                const el2 = findElement("#hillviewPage6 .topMenu #View #__buckets___");
                el2.click();
                (findElement(".dialog #x_buckets") as HTMLInputElement).value = "10";
                findElement(".dialog .confirm").click();
            },
        }, {
            description: "Quartiles vector",
            cond: () => Test.existsElement("#hillviewPage10 .idle"),
            cont: () => {
                const dest = findElement("#hillviewPage1 thead td[data-colname=Dest] .truncated");
                dest.click();
                const arrTime = findElement("#hillviewPage1 thead td[data-colname=ArrTime] .truncated");
                const ctrl = controlClickEvent();
                arrTime.dispatchEvent(ctrl);
                const evt = contextMenuEvent();
                arrTime.dispatchEvent(evt);
                const qv = findElement("#hillviewPage1 .dropdown #Quartile_vector");
                qv.click();
            }
        }, {
            description: "Stacked bars 2D histogram",
            cond: () => Test.existsElement("#hillviewPage11 .idle"),
            cont: () => {
                const carrier = findElement("#hillviewPage1 thead td[data-colname=UniqueCarrier] .truncated");
                carrier.click();
                const depDelay = findElement("#hillviewPage1 thead td[data-colname=DepDelay] .truncated");
                const ctrl = controlClickEvent();
                depDelay.dispatchEvent(ctrl);
                const evt = contextMenuEvent();
                depDelay.dispatchEvent(evt);
                const qv = findElement("#hillviewPage1 .dropdown #Histogram");
                qv.click();
            }
        }, {
            description: "Change buckets for 2D histogram",
            cond: () => Test.existsElement("#hillviewPage12 .idle"),
            cont: () => {
                const b = findElement("#hillviewPage12 .topMenu #View #__buckets___");
                b.click();
                (findElement(".dialog #y_buckets") as HTMLInputElement).value = "10";
                findElement(".dialog .confirm").click();
            }
        }, {
            description: "Non-stacked bars 2D histogram",
            cond: () => Test.existsElement("#hillviewPage12 .idle"),
            cont: () => {
                const b = findElement("#hillviewPage12 .topMenu #View #stacked_parallel");
                b.click();
                this.next();  // no interaction required
            }
        }, {
            description: "Close some windows",
            cond: () => Test.existsElement("#hillviewPage12 .idle"),
            cont: () => {
                /*
                    for (let i = 2; i < 8; i++) {
                        const el = findElement("#hillviewPage" + i.toString() + " .close");
                        if (el != null)
                            el.click();
                    }
                */
                const dialog = new NotifyDialog("Tests are completed", null, "Done.");
                dialog.show();
            },
        }
        ]);
    }
}
