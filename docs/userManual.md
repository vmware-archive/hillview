# Hillview User Manual

Hillview is a simple cloud-based spreadsheet program for browsing
large data collections.  Currently the data manipulated is read-only.
Users can sort, find, filter, transform, query, and chart data in some
simple ways; several operations are performed easily using direct
manipulation in the GUI.  Hillview is designed to work on very large
data sets (multi-million rows), complementing tools such as Excel.

Hillview attempts to provide fast data manipulation.  The illusion of
fast manipulation is provided by deferring work: Hillview only
computes as much of the data as must be shown to the user.  For
example, when sorting a dataset, it only sorts the rows currently
visible on the screen.  Hillview performs all operations using a class
of very efficient algorithms, called “Sketches”, which are constrained
to compute with bounded memory over distributed data.

## System architecture

The spreadsheet is a three-tier system, as shown in the following figure:

![System architecture](system-architecture.png)

* The user-interface (UI) runs in a browser.

* The service is exposed by a web server which runs in a datacenter on
  a head node.

* The service runs on a collection of servers in the datacenter;
  ideally these servers also store the data that is being browsed.

The Hillview service is implemented in Java.  The UI is written in
TypeScript.

Hillview can run as a federated system or loosel interconnected
components; the head node only sends small queries to the servers, and
it receivers small results from these.  The data on the server is
never sent over the network; the servers locally compute all results
that are needed.

## Streaming interaction

Users initiate operations in Hillview by performing operations within
the browser.  As a result of an operation, the back-end computes the
result as a series of approximations; these incremental results are
streamed back to the client and rendered, presenting the output with
gradually increasing precision.  Incremental results are always
accompanied by a progress bar, as in the following figure:

![Progress bar](progress.png)

Pressing the "stop" button next to a progress bar will cancel the
currently-executing operation.

## Data model

The Hillview data model is a large table, with a relatively small
number of columns (tens) and many rows (millions to billions).

The data in each column is typed; Hillview supports the following data
types:
  * String
  * Category (represented as strings)
  * JSON (represented as strings)
  * Double
  * Integer
  * DateTime
  * Interval

Hillview supports a special value "missing" which indicates that a
value is not present.  This is similar to NULL values in databases.

## Interacting with data

In this section we describe the various ways to present and interact
with the data.

### Loading data sets

*TODO*

The final UI for loading datasets is not yet implemented.  Currently
this UI consists in some buttons that load pre-defined datasets.

### Table views

The next figure shows a typical table view.

![Table user-interface](table-ui.png)

A table view displays only some of the columns of the data.  The
header of the visible columns is written in bold letters.  The
following image shows a table header with 15 columns out of which 3
are visible:

![Table header](table-header.png)

The data in the table is always sorted lexicographically on a
combination of the visible columns.  In the figure above the data is
sorted as follows:

* Data is sorted first on the Origin column in decreasing order (this
  is shown by the down-arrow next to the column name followed by a
  zero; this is the zero-th column in the sorting order)

* When two rows have the same Origin value, they are next compared on
  the UniqueCarrier column, also in decreasing order

* Finally, when two rows have the same value in the Origin and
  UniqueCarrier columns, they are next ordered by their value in the
  Cancelled column, also in decreasing order.

Initially a table view displays no columns.  The user can choose which
columns are displayed or hidden.

#### Selecting columns

The user can select one or more column using the mouse:
* Clicking on a column will select the column
* Clicking while pressing shift button will select a range of
  contiguous columns
* Clicking while pressing the control button will add or remove the
  current column from the selection

The following image shows the header of a table with 6 selected
columns.

![A table header with multiple selected columns](selected-column-header.png)

#### Operations on columns

Clicking on a column header pops up a menu that offers a set of
operations on the currently selected columns, as shown in the
following figure:

![Right-click menu for columns](column-right-click.png)

The contents of this menu may depend on the type of the column and on
the current state of the display.

* Sort ascending: The selected columns will be moved to the front of
  the sort order in ascending order.

* Sort descending: The selected columns will be moved to the front of
  the sort order in descending order.

* Heavy hitters...: This will start a heavy hitters computation on the
  selected columns.  *TODO*

* Heat map: this option requires exactly two columns of suitable types
  to be selected; in this case, it will draw a heatmap plot of the
  data in these two columns.  For Heatmaps see [Heat-map
  views](#heat-map-views).

* Hide: the selected columns will be removed from the sorting order
  and they will be hidden.

* Histogram: this option requires exactly one or two columns of
  suitable types to be selected.  If one column is selected, this menu
  will draw a histogram of the data in the selected column.  For
  one-dimensional histograms see
  [Uni-dimensional-histogram-views](#uni-dimensional histogram views).
  If two columns are selected this menu will draw a two-dimensional
  histogram of the data in the selected columns.  For two-dimensional
  histograms see [Two-dimensional
  histograms](#two-dimensional-histograms).

* Filter: this option will pop-up a dialog window that allows the user
  to filter the data.  *TODO*

*TODO*

### Uni-dimensional histogram views

*TODO*

### Two-dimensional histogram views

*TODO*

### Heat-map views

*TODO*