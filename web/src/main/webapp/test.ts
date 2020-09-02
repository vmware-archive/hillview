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
import {assert, findElement, findElementAny} from "./util";

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

    // noinspection JSMethodCanBeStatic
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
        const result = findElementAny(cssselector) != null;
        console.log("Checking element existence: " + cssselector + "=" + result);
        return result;
    }

    public createTestProgram(): void {
        /*
         This produces the following pages:
         First tab:
            syslog logs
         Second tab: ontime small dataset
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
            11: Quartiles plot
            12: Non-stacked bar charts plot
            13: Filtered table
            14: correlation heatmaps
            15: Trellis plot of quartiles
            16: Histogram of interval column
            17: Map view of OriginState
         */
        this.addProgram([{
            description: "Load rfc5424 logs",
            cond: () => true,
            cont: () => {
                findElement("#hillviewPage0 .topMenu #Generic_logs___").click();
                let formField = findElement(".dialog #fileNamePattern");
                (formField as HTMLInputElement).value = "data/sample_logs/rfc*";
                formField = findElement(".dialog #logFormat");
                (formField as HTMLInputElement).value = "%{RFC5424}";
                formField = findElement(".dialog #startTime");
                (formField as HTMLInputElement).value = "";
                formField = findElement(".dialog #endTime");
                (formField as HTMLInputElement).value = "";
                const confirm = findElement(".dialog .confirm");
                confirm.click();
            },
        }, {
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
                date.dispatchEvent(contextMenuEvent());
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
                cancCol.dispatchEvent(contextMenuEvent());
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
                col0.dispatchEvent(contextMenuEvent());
                findElement("#hillviewPage1 .dropdown #Show").click();
            },
        }, {
            description: "Show column 1",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                const col1 = findElement("#hillviewPage1 thead .col1");
                col1.dispatchEvent(contextMenuEvent());
                findElement("#hillviewPage1 .dropdown #Show").click();
            },
        }, {
            description: "Hide column 0",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                const col0 = findElement("#hillviewPage1 thead .col0");
                col0.dispatchEvent(contextMenuEvent());
                findElement("#hillviewPage1 .dropdown #Hide").click();
            },
        }, {
            description: "Create column in JavaScript",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                const cancCol = findElement("#hillviewPage1 thead td[data-colname=OriginCityName] .truncated");
                cancCol.dispatchEvent(contextMenuEvent());
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
                row1.dispatchEvent(controlClickEvent());
                // Add row 3
                const row3 = findElement("#hillviewPage2 #row3");
                row3.dispatchEvent(controlClickEvent());
                // Select menu item to show the associated table
                findElement("#hillviewPage2 .topMenu #Selected_columns").click();
                this.next(); // no rpc
            }
        }, {
            description: "Display histogram from schema view",
            cond: () => true,
            cont: () => {
                const col1 = findElement("#hillviewPage1 thead .col1");
                col1.dispatchEvent(contextMenuEvent());
                // Produces hillviewPage4
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            },
        }, {
            description: "Display a categorical histogram",
            cond: () => Test.existsElement("#hillviewPage4 .idle"),
            cont: () => {
                const col2 = findElement("#hillviewPage1 thead .col2");
                col2.dispatchEvent(contextMenuEvent());
                // Produces hillviewPage5
                findElement("#hillviewPage1 .dropdown #Histogram").click();
            },
        }, {
            description: "Display a pie chart",
            cond: () => Test.existsElement("#hillviewPage5 .idle"),
            cont: () => {
                const pie = findElement("#hillviewPage5 .topMenu #View #pie_chart_histogram");
                pie.click();
                this.next();
            },
        }, {
            description: "Display a 2D histogram",
            cond: () => Test.existsElement("#hillviewPage5 .idle"),
            cont: () => {
                findElement("#hillviewPage1 thead .col8").click();
                const col9 = findElement("#hillviewPage1 thead .col9");
                col9.dispatchEvent(controlClickEvent());
                col9.dispatchEvent(contextMenuEvent());
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
                const col2 = findElement("#hillviewPage1 thead .col2");
                col2.dispatchEvent(contextMenuEvent());
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
                arrTime.dispatchEvent(controlClickEvent());
                arrTime.dispatchEvent(contextMenuEvent());
                const qv = findElement("#hillviewPage1 .dropdown #Quartiles");
                qv.click();
            }
        }, {
            description: "Stacked bars 2D histogram",
            cond: () => Test.existsElement("#hillviewPage11 .idle"),
            cont: () => {
                const carrier = findElement("#hillviewPage1 thead td[data-colname=UniqueCarrier] .truncated");
                carrier.click();
                const depDelay = findElement("#hillviewPage1 thead td[data-colname=DepDelay] .truncated");
                depDelay.dispatchEvent(controlClickEvent());
                depDelay.dispatchEvent(contextMenuEvent());
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
            description: "Filter based on the first table cell value",
            cond: () => Test.existsElement("#hillviewPage12 .idle"),
            cont: () => {
                const cell = findElement("#hillviewPage1 [data-col=\"3\"][data-row=\"1\"]");
                cell.dispatchEvent(contextMenuEvent());
                const menu = findElement("#hillviewPage1 #Keep_2016_01_01");
                menu.click();
            }
        }, {
            description: "Correlation heatmaps",
            cond: () => Test.existsElement("#hillviewPage13 .idle"),
            cont: () => {
                const cellDep = findElement("#hillviewPage1 thead td[data-colname=DepTime] .truncated");
                cellDep.click();
                cellDep.scrollIntoView();
                const cellArr = findElement("#hillviewPage1 thead td[data-colname=ArrDelay] .truncated");
                cellArr.dispatchEvent(mouseClickEvent(true, false));
                cellArr.dispatchEvent(contextMenuEvent());
                const qv = findElement("#hillviewPage1 .dropdown #Correlation");
                qv.click();
            }
        }, {
            description: "Trellis plots of quartile vectors",
            cond: () => Test.existsElement("#hillviewPage14 .idle"),
            cont: () => {
                findElement("#hillviewPage11 .topMenu #View #group_by___").click();
                (findElement(".dialog .confirm")).click();
            }
        }, {
            description: "Create interval column",
            cond: () => Test.existsElement("#hillviewPage15 .idle"),
            cont: () => {
                // click on a new cell, to deselect existing ones
                const other = findElement("#hillviewPage1 thead td[data-colname=DayOfWeek] .truncated");
                other.scrollIntoView();
                other.click();
                // start selection
                const cellDep = findElement("#hillviewPage1 thead td[data-colname=DepTime] .truncated");
                cellDep.click();
                const cellArr = findElement("#hillviewPage1 thead td[data-colname=ArrTime] .truncated");
                cellArr.dispatchEvent(controlClickEvent());
                cellArr.dispatchEvent(contextMenuEvent());
                const qv = findElement("#hillviewPage1 .dropdown #Create_interval_column___");
                qv.click();
                (findElement(".dialog .confirm")).click();
            }
        }, {
            description: "Histogram of interval column",
            cond: () => Test.existsElement("#hillviewPage1 .idle"),
            cont: () => {
                findElement("#hillviewPage1 #Chart").click();
                findElement("#I1D_Histogram___").click();
                (findElement(".dialog #columnName") as HTMLInputElement).value = "DepTime:ArrTime";
                findElement(".dialog .confirm").click();
            },
        }, {
            description: "Map of OriginState",
            cond: () => Test.existsElement("#hillviewPage16 .idle"),
            cont: () => {
                const other = findElement("#hillviewPage1 thead td[data-colname=OriginState] .truncated");
                other.click();
                other.dispatchEvent(contextMenuEvent());
                const qv = findElement("#hillviewPage1 .dropdown #Map");
                qv.click();
            },
        }, {
            description: "Close some windows",
            cond: () => Test.existsElement("#hillviewPage17 .idle"),
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
