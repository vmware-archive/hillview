import {IColumnDescription} from "../table"
import {BasicColStats} from "../histogramBase"

export class HeatMapArrayData {
    buckets: number[][][];
    missingData: number;
    totalsize: number;
}

export interface HeatMapArrayArgs {
    cds: IColumnDescription[];
    uniqueStrings?: string[];
    zBins?: string[];
    subsampled?: boolean;
    xStats?: BasicColStats;
    yStats?: BasicColStats;
}
