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
example, when sorting a dataset, Sketch only sorts the rows currently
visible on the screen.  Hillview performs all operations using a class
of very efficient algorithms, called “Sketches”, which are constrained
to compute with bounded memory over distributed data.

## System architecture

The spreadsheet is a three-tier system, as showin in the following figure:

![System architecture](system-architecture.png)

* The user-interface (UI) runs in a browser.

* The service is exposed by a web server which runs in a datacenter on
  a head node.

* The service runs on a collection of servers in the datacenter;
  ideally these servers also store the data that is being browsed.

The Hillview service is implemented in Java.

## Streaming interaction

Users initiate operations in Hillview by performing operations within
the browser.  As a result of an operation, the back-end computes the
result as a series of approximations of increasing precision; these
incremental results are streamed back to the client.  The UI presents
the results as they arrive, updating incrementally the view.
Incremental results are always accompanied by a progress bar, as in
the following figure:

![Progress bar](progress.pgn)

Pressing the "stop" button will cancel the currently-executing
operation.

## Interacting with data

In this section we describe the various ways to present and interact
with the data.

### Loading data sets

*TODO* The UI for loading datasets is not yet implemented.  Currently
this UI consists in some buttons that load pre-defined datasets.

#### Data schema

A schema specifies for each column three attributes:
* The column name
* Whether the column can contain missing values
• The column type; supported types are:
  * String
  * Category (represented as strings)
  * JSON (represented as strings)
  * Double
  * Integer
  * LocalDateTime
  * Interval

### Table views

The next figure shows a typical table view.

![Table user-interface](table-ui.png)

A table view displays only some of the columns of the data.  The
header of the visible columns is written in bold letters.  The
following image shows a table header:

![Table header](table-header.png)

The data in the table is always sorted lexicographically on a
combination of the visible columns.  In this figure the data is sorted
as follows:

* Data is sorted first on the Origin column in decreasing order (this
  is shown by the down-arrow next to the column name followed by a
  zero; this is the zero-th column in the sorting order)

* When two rows have the same origin value, they are next compared on
  the UniqueCarrier column, also in decreasing order

* Finally, when two rows have the same value in the first two columns,
  they are next ordered by their value in the Cancelled column, also
  in decreasing order.

Initially a table view displays no columns.

#### Selecting columns

The user can select one or more column using the mouse:
* Clicking on a column will select the column
* Clicking while pressing shift button will select a range of
  contiguous columns
* Clicking while pressing the control button will add or remove the
  current column from the selection

Clicking on a column header pops up a menu that offers a set of
operations on the currently selected columns, as shown in the
following figure:

![Right-click menu for columns](column-right-click.png)

*TODO*

### Uni-dimensional histograms views

*TODO*

### Two-dimensional histograms views

*TODO*

### Heat-map views

*TODO*