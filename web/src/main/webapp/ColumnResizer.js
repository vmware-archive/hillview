/**
 * This is a modified clone of https://github.com/MonsantoCo/column-resizer
 * Created by jjglyn on 12/19/16.
 */
import stringHash from 'string-hash';

const counter = (() => {
    let count = 0;
    return () => {
        return count++;
    };
})();

export default class ColumnResizer {
    ID = 'id';
    PX = 'px';
    RESIZABLE = 'grip-resizable';
    FLEX = 'grip-flex';
    legacyIE = navigator.userAgent.indexOf('Trident/4.0') > 0;

    /**
     *
     * @param {HTMLTableElement} tb
     * @param {Object} options
     */
    constructor(tb, options = {}) {
        try {
            this.store = sessionStorage;
        } catch (e) {
            this.store = {};
        }
        this.grip = null;
        this.tb = tb;
        window.addEventListener('resize', this.onResize);
        // Polyfill for IE
        if (!Element.prototype.matches) {
            Element.prototype.matches = Element.prototype.msMatchesSelector;
        }
        this.init(options);
    }

    /**
     * Reinitialize the object with options.
     * @param {Object} options
     * @returns {Object} previous options object if any
     */
    reset = options => {
        return this.init(options);
    };

    /**
     * Remove column resizing properties from the table then re-apply them
     */
    onResize = () => {
        const t = this.tb;
        t.classList.remove(this.RESIZABLE);
        if (t.opt.fixed) {
            t.tableWidth = Number(window.getComputedStyle(t).width.replace(/px/, '')).valueOf();
            let mw = 0;
            for (let i = 0; i < t.columnCnt; i++) {
                mw += t.columns[i].w;
            }
            for (let i = 0; i < t.columnCnt; i++) {
                t.columns[i].style.width = Math.round(1000 * t.columns[i].w / mw) / 10 + '%';
                t.columns[i].locked = true;
            }
        } else {
            this.applyBounds();
            if (t.opt.resizeMode === 'flex') {
                this.serializeStore();
            }
        }
        t.classList.add(this.RESIZABLE);
        this.syncGrips();
    };

    /**
     * Event handler fired when the grip's dragging is about to start. Its main goal is to set up events
     * and store some values used while dragging.
     * @param {UIEvent} e - grip's mousedown/touchstart event
     */
    onGripMouseDown = (e) => {
        const o = e.target.parentNode.data;
        const t = this.tb;
        const g = t.grips[o.i];
        const oe = e.touches;
        g.ox = oe ? oe[0].pageX : e.pageX;
        g.l = g.offsetLeft;
        g.x = g.l;

        this.createStyle(document.querySelector('head'), '*{cursor:' + t.opt.dragCursor + '!important}');
        document.addEventListener('touchmove', this.onGripDrag);
        document.addEventListener('mousemove', this.onGripDrag);
        document.addEventListener('touchend', this.onGripDragOver);
        document.addEventListener('mouseup', this.onGripDragOver);
        g.classList.add(t.opt.draggingClass); 	//add the dragging class (to allow some visual feedback)
        this.grip = g;
        if (t.columns[o.i].locked) { 	//if the column is locked (after browser resize), then c.w must be updated
            for (let i = 0, c; i < t.columnCnt; i++) {
                c = t.columns[i];
                c.locked = false;
                c.w = Number(window.getComputedStyle(c).width.replace(/px/, '')).valueOf();
            }
        }
        e.preventDefault(); //prevent text selection
    };

    /**
     * Event handler used while dragging a grip. It checks if the next grip's position is valid and updates it.
     * @param {UIEvent} e - mousemove/touchmove event bound to the window object
     */
    onGripDrag = (e) => {
        const grip = this.grip;
        if (!grip) {
            return;
        }
        const t = grip.t;
        const oe = e.touches;
        const ox = oe ? oe[0].pageX : e.pageX;
        let x = ox - grip.ox + grip.l;
        const mw = t.opt.minWidth;
        const i = grip.i;
        const l = t.cellSpace * 1.5 + mw + t.borderSpace;
        const last = i === t.columnCnt - 1;
        const min = i ? t.grips[i - 1].offsetLeft + t.cellSpace + mw : l;
        const max = t.opt.fixed ? i === t.columnCnt - 1 ? t.tableWidth - l : t.grips[i + 1].offsetLeft - t.cellSpace - mw : Infinity;
        x = Math.max(min, Math.min(max, x));
        grip.x = x;
        grip.style.left = x + this.PX;
        if (last) {
            grip.w = t.columns[i].w + x - grip.l;
        }
        if (t.opt.liveDrag) {
            if (last) {
                t.columns[i].style.width = grip.w + this.PX;
                if (!t.opt.fixed && t.opt.overflow) {
                    t.style.minWidth = (t.tableWidth + x - grip.l) + this.PX;
                } else {
                    t.tableWidth = Number(window.getComputedStyle(t).width.replace(/px/, '')).valueOf();
                }
            } else {
                this.syncCols(t, i, false, t.opt);
            }
            this.syncGrips();
            const cb = t.opt.onDrag;
            if (cb) {
                cb(e);
            }
        }
        e.preventDefault(); //prevent text selection while dragging
    };

    /**
     * Event handler fired when the dragging is over, updating table layout
     * @param {UIEvent} e - grip's drag over event
     */
    onGripDragOver = (e) => {
        const grip = this.grip;
        document.removeEventListener('touchend', this.onGripDragOver);
        document.removeEventListener('mouseup', this.onGripDragOver);
        document.removeEventListener('touchmove', this.onGripDrag);
        document.removeEventListener('mousemove', this.onGripDrag);
        const last = document.querySelector('head').lastChild;
        last.parentNode.removeChild(last);
        if (!grip) {
            return;
        }
        grip.classList.remove(grip.t.opt.draggingClass);
        if (!(grip.x - grip.l === 0)) {
            const t = grip.t;
            const cb = t.opt.onResize;
            const i = grip.i;
            const last = i === t.columnCnt - 1;
            if (last) {
                const c = t.columns[i];
                c.style.width = grip.w + this.PX;
                c.w = grip.w;
            } else {
                this.syncCols(t, i, true, t.opt);
            }
            if (!t.opt.fixed) {
                this.applyBounds();
            }
            this.syncGrips();
            if (cb) {
                cb(e);
            }
            this.serializeStore();
        }
        this.grip = null;
    };

    /**
     * Prepares the table set in the constructor for resizing.
     * @param {Object} options
     * @returns {Object} previous options object if any
     */
    init = (options) => {
        if (options.disable) {
            return this.destroy();
        }
        const tb = this.tb;
        const id = tb.getAttribute(this.ID) || this.RESIZABLE + counter();
        if (!tb.matches('table') || tb.extended && !options.partialRefresh) {
            return null;
        }
        //append required CSS rules
        const head = document.querySelector('head');
        const css = ' .grip-resizable{table-layout:fixed;} .grip-resizable > tbody > tr > td, .grip-resizable > tbody > tr > th{overflow:hidden}'
            + ' .grip-padding > tbody > tr > td, .grip-padding > tbody > tr > th{padding-left:0!important; padding-right:0!important;}'
            + ' .grip-container{ height:0px; position:relative;} .grip-handle{margin-left:-5px; position:absolute; z-index:5; }'
            + ' .grip-handle .grip-resizable{position:absolute;background-color:red;filter:alpha(opacity=1);opacity:0;width:10px;height:100%;cursor: col-resize;top:0px}'
            + ' .grip-lastgrip{position:absolute; width:1px; } .grip-drag{ border-left:1px dotted black;	}'
            + ' .grip-flex{} .grip-handle.grip-disabledgrip .grip-resizable{cursor:default; display:none;}';
        this.createStyle(head, css);
        if (options.hoverCursor && options.hoverCursor !== 'col-resize') {
            const css = '.grip-handle .grip-resizable:hover{cursor:' + options.hoverCursor + '!important}';
            this.createStyle(head, css);
        }
        tb.setAttribute(this.ID, id);
        const oldOptions = tb.opt;
        tb.opt = this.extendOptions(options);
        const headers = this.getTableHeaders(tb);
        this.extendTable(headers);
        if (options.remoteTable && options.remoteTable.matches('table')) {
            const remoteHeaders = this.getTableHeaders(tb.opt.remoteTable);
            if (headers.length === remoteHeaders.length) {
                this.extendRemoteTable(tb.opt.remoteTable, remoteHeaders, tb);
            } else {
                console.warn('column count for remote table did not match');
            }
        }
        return oldOptions;
    };

    /**
     * This function updates all columns width according to its real width. It must be taken into account that the
     * sum of all columns can exceed the table width in some cases (if fixed is set to false and table has some kind
     * of max-width).
     */
    applyBounds = () => {
        const t = this.tb;
        const w = t.columns.map(col => {
            return window.getComputedStyle(col).width;
        });
        t.style.width = window.getComputedStyle(t).width;
        t.tableWidth = Number(t.style.width.replace(/px/, '')).valueOf();
        //prevent table width changes
        t.classList.remove(this.FLEX);
        t.columns.forEach((col, i) => {
            col.style.width = w[i];
            col.w = Number(w[i].replace(/px/, '')).valueOf();
        });
        //allow table width changes
        t.classList.add(this.FLEX);
    };

    /**
     * Writes the current column widths to storage.
     */
    serializeStore = () => {
        const store = this.store;
        const t = this.tb;
        store[t.getAttribute(this.ID)] = '';
        let m = 0;
        for (let i = 0; i < t.columns.length; i++) {
            const w = window.getComputedStyle(t.columns[i]).width.replace(/px/, '');
            store[t.getAttribute(this.ID)] += w + ';';
            m += Number(w).valueOf();
        }
        //the last item of the serialized string is the table's active area (width)
        store[t.getAttribute(this.ID)] += m.toString();
        if (!t.opt.fixed) {
            store[t.getAttribute(this.ID)] += ';' + window.getComputedStyle(t).width.replace(/px/, '');
        }
    };

    /**
     * Function that places each grip in the correct position according to the current table layout
     */
    syncGrips = () => {
        const t = this.tb;
        t.gripContainer.style.width = t.tableWidth + this.PX;
        for (let i = 0; i < t.columnCnt; i++) {
            const c = t.columns[i];
            t.opt.widths[i] = c.w;
            const cRect = c.getBoundingClientRect();
            const tRect = t.getBoundingClientRect();
            t.grips[i].style.left = cRect.left - tRect.left + c.offsetWidth + t.cellSpace / 2 + this.PX;
            t.grips[i].style.height = (t.opt.headerOnly ? t.columns[0].offsetHeight : t.offsetHeight) + this.PX;
        }
    };

    /**
     * This function removes any enhancements from the table being processed.
     * @returns {Object} current option object if any
     */
    destroy = () => {
        const tt = this.tb;
        const id = tt.getAttribute(this.ID);
        if (!id) {
            return null;
        }
        this.store[id] = '';
        tt.classList.remove(this.RESIZABLE);
        tt.classList.remove(this.FLEX);
        if (tt.remote) {
            tt.remote.classList.remove(this.RESIZABLE);
            tt.remote.classList.remove(this.FLEX);
        }
        if (tt.gripContainer && tt.gripContainer.parentNode) {
            tt.gripContainer.parentNode.removeChild(tt.gripContainer);
        }
        delete tt.extended;
        return tt.opt;
    };

    /**
     * Utility method to add a <style> to an element
     * @param {HTMLElement} element
     * @param {string} css
     */
    createStyle = (element, css) => {
        const hash = stringHash(css).toString();
        const oldStyle = element.querySelectorAll('style');
        const filtered = Array.from(oldStyle).filter(style => {
            return (style.gripid === hash);
        });
        if (filtered.length) {
            return;
        }
        const style = document.createElement('style');
        style.type = 'text/css';
        style.gripid = hash;
        if (style.styleSheet) {
            style.styleSheet.cssText = css;
        } else {
            style.appendChild(document.createTextNode(css));
        }
        element.appendChild(style);
    };

    /**
     * Populates unset options with defaults and sets resizeMode properties.
     * @param {Object} options
     * @returns {Object}
     */
    extendOptions = (options) => {
        const extOptions = Object.assign({}, ColumnResizer.DEFAULTS, options);
        extOptions.fixed = true;
        extOptions.overflow = false;
        switch (extOptions.resizeMode) {
            case 'flex':
                extOptions.fixed = false;
                break;
            case 'overflow':
                extOptions.fixed = false;
                extOptions.overflow = true;
                break;
        }
        return extOptions;
    };

    /**
     * Finds all the visible table header elements from a given table.
     * @param {HTMLTableElement} table
     * @returns {HTMLElement[]}
     */
    getTableHeaders = (table) => {
        const id = '#' + table.id;
        let th = Array.from(table.querySelectorAll(id + '>thead>tr:nth-of-type(1)>th'));
        th = th.concat(Array.from(table.querySelectorAll(id + '>thead>tr:nth-of-type(1)>td')));
        if (!th.length) {
            th = Array.from(table.querySelectorAll(id + '>tbody>tr:nth-of-type(1)>th'));
            th = th.concat(Array.from(table.querySelectorAll(id + '>tr:nth-of-type(1)>th')));
            th = th.concat(Array.from(table.querySelectorAll(id + '>tbody>tr:nth-of-type(1)>td')));
            th = th.concat(Array.from(table.querySelectorAll(id + '>tr:nth-of-type(1)>td')));
        }
        return this.filterInvisible(th, false);
    };

    /**
     * Filter invisible columns.
     * @param {HTMLElement[]} nodes
     * @param {boolean} column
     * @return {HTMLElement[]}
     */
    filterInvisible = (nodes, column) => {
        return nodes.filter((node) => {
            const width = column ? -1 : node.offsetWidth;
            const height = column ? -1 : node.offsetHeight;
            const invisible = (width === 0 && height === 0)
                || (node.style && node.style.display && window.getComputedStyle(node).display === 'none') || false;
            return !invisible;
        });
    };

    /**
     * Add properties to the table for resizing
     * @param {HTMLTableElement} th
     */
    extendTable = (th) => {
        const tb = this.tb;
        if (tb.opt.removePadding) {
            tb.classList.add('grip-padding');
        }
        tb.classList.add(this.RESIZABLE);
        tb.insertAdjacentHTML('beforebegin', '<div class="grip-container"/>');	//class forces table rendering in fixed-layout mode to prevent column's min-width
        tb.grips = []; // grips
        tb.columns = []; // columns
        tb.tableWidth = Number(window.getComputedStyle(tb).width.replace(/px/, '')).valueOf();
        tb.gripContainer = tb.previousElementSibling;
        if (tb.opt.marginLeft) {
            tb.gripContainer.style.marginLeft = tb.opt.marginLeft;
        }
        if (tb.opt.marginRight) {
            tb.gripContainer.style.marginRight = tb.opt.marginRight;
        }
        tb.cellSpace = parseInt(this.legacyIE ? tb.cellSpacing || tb.currentStyle.borderSpacing : window.getComputedStyle(tb).borderSpacing.split(' ')[0].replace(/px/, '')) || 2;
        tb.borderSpace = parseInt(this.legacyIE ? tb.border || tb.currentStyle.borderLeftWidth : window.getComputedStyle(tb).borderLeftWidth.replace(/px/, '')) || 1;
        tb.extended = true;
        this.createGrips(th);
    };

    /**
     * Add properties to the remote table for resizing
     * @param {HTMLTableElement} tb - the remote table
     * @param {HTMLElement[]} th - table header array
     * @param {HTMLTableElement} controller - the controlling table
     */
    extendRemoteTable = (tb, th, controller) => {
        const options = controller.opt;
        if (options.removePadding) {
            tb.classList.add('grip-padding');
        }
        tb.classList.add(this.RESIZABLE);
        if (!tb.getAttribute(this.ID)) {
            tb.setAttribute(this.ID, controller.getAttribute(this.ID) + 'remote');
        }
        tb.columns = []; // columns
        th.forEach((header, index) => {
            const column = th[index];
            column.w = controller.columns[index].w;
            column.style.width = column.w + this.PX;
            column.removeAttribute('width');
            tb.columns.push(column);
        });
        tb.tableWidth = controller.tableWidth;
        tb.cellSpace = controller.cellSpace;
        tb.borderSpace = controller.borderSpace;
        const cg = Array.from(tb.querySelectorAll('col'));
        tb.columnGrp = this.filterInvisible(cg, true);
        tb.columnGrp.forEach( (col, index) => {
            col.removeAttribute('width');
            col.style.width = controller.columnGrp[index].style.width;
        });
        controller.remote = tb;
    };

    /**
     * Function to create all the grips associated with the table given by parameters
     * @param {HTMLElement[]} th - table header array
     */
    createGrips = (th) => {
        const t = this.tb;
        t.columnGrp = this.filterInvisible(Array.from(t.querySelectorAll('col')), true);
        t.columnGrp.forEach(col => {
            col.removeAttribute('width');
        });
        t.columnCnt = th.length;
        if (this.store[t.getAttribute(this.ID)]) {
            this.deserializeStore(th);
        }
        if (!t.opt.widths) {
            t.opt.widths = [];
        }
        th.forEach((header, index) => {
            const column = th[index];
            const dc = t.opt.disabledColumns.indexOf(index) !== -1;
            this.createDiv(t.gripContainer, 'grip-handle');
            const handle = t.gripContainer.lastChild;
            if (!dc && t.opt.gripInnerHtml) { //add the visual node to be used as grip
                handle.innerHTML = t.opt.gripInnerHtml;
            }
            this.createDiv(handle, this.RESIZABLE);
            if (index === t.columnCnt - 1) {
                handle.classList.add('grip-lastgrip');
                if (t.opt.fixed) {
                    // if the table resizing mode is set to fixed, the last grip is removed since table
                    // width can not change
                    handle.innerHTML = '';
                }
            }
            handle.addEventListener('touchstart', this.onGripMouseDown, {capture: true, passive: true});
            handle.addEventListener('mousedown', this.onGripMouseDown, true);

            if (!dc) {
                handle.classList.remove('grip-disabledgrip');
                handle.addEventListener('touchstart', this.onGripMouseDown, {capture: true, passive: true});
                handle.addEventListener('mousedown', this.onGripMouseDown, true);
            } else {
                handle.classList.add('grip-disabledgrip');
            }

            handle.t = t;
            handle.i = index;
            if (t.opt.widths[index]) {
                column.w = t.opt.widths[index];
            } else {
                column.w = Number(window.getComputedStyle(column).width.replace(/px/, '')).valueOf();
                t.opt.widths[index] = column.w;
            }
            column.style.width = column.w + this.PX;
            column.removeAttribute('width');
            handle.data = {i: index, t: t.getAttribute(this.ID), last: index === t.columnCnt - 1};
            t.grips.push(handle);
            t.columns.push(column);
        });
        let ot = Array.from(t.querySelectorAll('td'));
        ot.concat(Array.from(t.querySelectorAll('th')));
        //the width attribute is removed from all table cells which are not nested in other tables and don't belong to the header array
        ot = ot.filter((node) => {
            // .not(th)
            for (let i = 0; i < th.length; i++) {
                if (th[i] === node) return false;
            }
            return true;
        });
        ot = ot.filter((node) => {
            //.not('table th, table td')
            return !(node.querySelectorAll('table th').length || node.querySelectorAll('table td').length);
        });
        ot.forEach(table => {
            table.removeAttribute('width');
        });
        if (!t.opt.fixed) {
            t.removeAttribute('width');
            t.classList.add(this.FLEX);
        }
        this.syncGrips();
    };

    /**
     * Get the stored table headers.
     * @param {HTMLElement[]} th - table header array
     */
    deserializeStore = (th) => {
        const t = this.tb;
        t.columnGrp.forEach((node) => {
            node.removeAttribute('width');
        });
        if (t.opt.flush) {
            this.store[t.getAttribute(this.ID)] = '';
            return;
        }
        const w = this.store[t.getAttribute(this.ID)].split(';');
        const tw = w[t.columnCnt + 1];
        if (!t.opt.fixed && tw) {
            t.style.width = tw + this.PX;
            if (t.opt.overflow) {
                t.style.minWidth = tw + this.PX;
                t.tableWidth = Number(tw).valueOf();
            }
        }
        for (let i = 0; i < t.columnCnt; i++) {
            const width = 100 * Number(w[i]).valueOf() / Number(w[t.columnCnt]).valueOf() + '%';
            th[i].style.width = width;
            if (t.columnGrp[i]) {
                // this code is required in order to create an inline CSS rule with higher precedence than
                // an existing CSS class in the 'col' elements
                t.columnGrp[i].style.width = width;
            }
        }
    };

    /**
     * Utility method to wrap HTML text in a <div/> and appent to an element.
     * @param {HTMLElement} element - the HTML element to append the div to
     * @param {string} className - class name for the new div for styling
     * @param {string} text - inner HTML text
     */
    createDiv = (element, className, text) => {
        const div = document.createElement('div');
        div.classList.add(className);
        if (text) {
            div.innerHTML = text;
        }
        element.appendChild(div);
    };

    /**
     * This function updates column's width according to the horizontal position increment of the grip being
     * dragged. The function can be called while dragging if liveDragging is enabled and also from the onGripDragOver
     * event handler to synchronize grip's position with their related columns.
     * @param {HTMLTableElement} t - table object
     * @param {number} i - index of the grip being dragged
     * @param {boolean} isOver - to identify when the function is being called from the onGripDragOver event
     * @param {Object} options - used for chaining options with remote tables
     */
    syncCols = (t, i, isOver, options) => {
        const remote = t.remote;
        const inc = this.grip.x - this.grip.l;
        const c0 = t.columns[i];
        const c1 = t.columns[i + 1];
        if (!(c0 && c1)) {
            return;
        }
        const w0 = c0.w + inc;
        const w1 = c1.w - inc;
        const sw0 = w0 + this.PX;
        c0.style.width = sw0;
        if (t.columnGrp[i] && t.columnGrp[i].style.width) {
            t.columnGrp[i].style.width = sw0;
        }
        if (remote) {
            remote.columns[i].style.width = sw0;
            if (remote.columnGrp[i] && remote.columnGrp[i].style.width) {
                remote.columnGrp[i].style.width = sw0;
            }
        }
        if (options.fixed) {
            const sw1 = w1 + this.PX;
            c1.style.width = sw1;
            if (t.columnGrp[i + 1] && t.columnGrp[i + 1].style.width) {
                t.columnGrp[i + 1].style.width = sw1;
            }
            if (remote) {
                remote.columns[i + 1].style.width = sw1;
                if (remote.columnGrp[i + 1] && remote.columnGrp[i + 1].style.width) {
                    remote.columnGrp[i + 1].style.width = sw1;
                }
            }
        } else if (options.overflow) {
            //if overflow is set, increment min-width to force overflow
            t.style.minWidth = (t.tableWidth + inc) + this.PX;
        }
        if (isOver) {
            c0.w = w0;
            c1.w = options.fixed ? w1 : c1.w;
            if (remote) {
                remote.columns[i].w = w0;
                remote.columns[i + 1].w = options.fixed ? w1 : c1.w;
            }
        }
    };
}

ColumnResizer.DEFAULTS = {
    //attributes:
    resizeMode: 'fit',              //mode can be 'fit', 'flex' or 'overflow'
    draggingClass: 'grip-drag',	    //css-class used when a grip is being dragged (for visual feedback purposes)
    gripInnerHtml: '',				//if it is required to use a custom grip it can be done using some custom HTML
    liveDrag: false,				//enables table-layout updating while dragging
    minWidth: 15, 					//minimum width value in pixels allowed for a column
    headerOnly: false,				//specifies that the size of the the column resizing anchors will be bounded to the size of the first row
    hoverCursor: 'col-resize',  	//cursor to be used on grip hover
    dragCursor: 'col-resize',  		//cursor to be used while dragging
    flush: false, 					//when it is required to prevent layout restoration after postback, 'flush' will remove its associated layout data
    marginLeft: null,				//e.g. '10%', '15em', '5px' ...
    marginRight: null, 				//e.g. '10%', '15em', '5px' ...
    remoteTable: null,              //other table element to resize using the main table as a controller
    disable: false,					//disables all the enhancements performed in a previously resized table
    partialRefresh: false,			//can be used when the table is inside of an updatePanel,
    disabledColumns: [],            //column indexes to be excluded
    removePadding: true,            //remove padding from the header cells.
    widths: [],                     //array of initial column widths

    //events:
    onDrag: null, 					//callback function to be fired during the column resizing process if liveDrag is enabled
    onResize: null					//callback function fired when the dragging process is over
};
