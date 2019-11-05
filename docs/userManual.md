<!-- automatically generated from userManual.src by doc/number-sections.py-->
# Hillview User Manual

[Hillview](https://github.com/vmware/hillview) is a simple cloud-based
spreadsheet program for browsing large data collections.  Currently
the data manipulated is read-only.  Users can sort, find, filter,
transform, query, zoom-in/out, and chart data in simple ways;
operations are performed easily using direct manipulation in the GUI.
Hillview is designed to work on very large data sets (billions of
rows), with an interactive spreadsheet-like interaction style,
complementing sophisticated analytic engines.  Hillview can also be
executed as a stand-alone executable on a local machine (but then the
data size it can manipulate is limited by the available machine
resources).

Hillview attempts to provide fast data manipulation.  The speed is
obtained by deferring work: Hillview only computes as much of the data
as must be shown to the user.  For example, when sorting a dataset, it
only sorts the rows currently visible on the screen.  Hillview
performs all operations using a class of very efficient algorithms,
called “sketches”, which are constrained to compute with bounded
memory over distributed data.

# Contents
|Section|Reference|
|---:|:---|
|1.|[Basic concepts](#1-basic-concepts)|
|1.1.|[System architecture](#11-system-architecture)|
|1.2.|[Streaming interaction](#12-streaming-interaction)|
|1.3.|[Data model](#13-data-model)|
|2.|[Interacting with data](#2-interacting-with-data)|
|2.1.|[Error display](#21-error-display)|
|2.2.|[Mouse-based selection](#22-mouse-based-selection)|
|2.3.|[Loading data](#23-loading-data)|
|2.3.1.|[Specifying the data schema](#231-specifying-the-data-schema)|
|2.3.2.|[Specifying rules for parsing logs](#232-specifying-rules-for-parsing-logs)|
|2.3.2.1.|[Example](#2321-example)|
|2.3.2.2.|[Regular Expressions](#2322-regular-expressions)|
|2.3.2.3.|[Custom Patterns](#2323-custom-patterns)|
|2.3.3.|[Reading generic logs](#233-reading-generic-logs)|
|2.3.4.|[Reading saved data views](#234-reading-saved-data-views)|
|2.3.5.|[Reading CSV files](#235-reading-csv-files)|
|2.3.6.|[Reading JSON files](#236-reading-json-files)|
|2.3.7.|[Reading ORC files](#237-reading-orc-files)|
|2.3.8.|[Reading data from SQL databases](#238-reading-data-from-sql-databases)|
|2.3.9.|[Reading Parquet files](#239-reading-parquet-files)|
|2.4.|[Navigating multiple datasets](#24-navigating-multiple-datasets)|
|3.|[Data views](#3-data-views)|
|3.1.|[Schema views](#31-schema-views)|
|3.1.1.|[Selecting columns](#311-selecting-columns)|
|3.1.2.|[The schema view menu](#312-the-schema-view-menu)|
|3.1.3.|[The chart menu](#313-the-chart-menu)|
|3.1.4.|[Saving data](#314-saving-data)|
|3.2.|[Table views](#32-table-views)|
|3.2.1.|[Scrolling](#321-scrolling)|
|3.2.2.|[Selecting columns](#322-selecting-columns)|
|3.2.3.|[Operations on selected columns](#323-operations-on-selected-columns)|
|3.2.4.|[Operations on a table cell](#324-operations-on-a-table-cell)|
|3.2.5.|[Operations on tables](#325-operations-on-tables)|
|3.2.6.|[The table view menu](#326-the-table-view-menu)|
|3.2.7.|[The table filter menu](#327-the-table-filter-menu)|
|3.3.|[Frequent elements views](#33-frequent-elements-views)|
|3.3.1.|[View as a Table](#331-view-as-a-table)|
|3.3.2.|[Modify](#332-modify)|
|3.4.|[Uni-dimensional histogram views](#34-uni-dimensional-histogram-views)|
|3.4.1.|[The histogram menu](#341-the-histogram-menu)|
|3.4.2.|[The histogram view menu](#342-the-histogram-view-menu)|
|3.4.3.|[Mouse selection in histogram views](#343-mouse-selection-in-histogram-views)|
|3.4.4.|[String histograms](#344-string-histograms)|
|3.5.|[Two-dimensional histogram views](#35-two-dimensional-histogram-views)|
|3.5.1.|[Selection in 2D histograms](#351-selection-in-2d-histograms)|
|3.6.|[Heatmap views](#36-heatmap-views)|
|3.6.1.|[Selection from a heatmap](#361-selection-from-a-heatmap)|
|3.7.|[Singular value spectrum views](#37-singular-value-spectrum-views)|
|4.|[Trellis plots](#4-trellis-plots)|
|4.1.|[Trellis plots of 1D histograms](#41-trellis-plots-of-1d-histograms)|
|4.2.|[Trellis plots of 2D histograms](#42-trellis-plots-of-2d-histograms)|
|4.3.|[Trellis plots of heatmaps](#43-trellis-plots-of-heatmaps)|
|4.4.|[Combining two views](#44-combining-two-views)|
## 1. Basic concepts

### 1.1. System architecture

Hillview's architecture is shown in the following figure:

![System architecture](system-architecture.png)

* The user-interface (UI) runs in a browser.

* The service is exposed by a web server which runs in a datacenter on
  a root node.

* The service runs on a collection of worker servers in the
  datacenter.  These servers must be able to read the browsed data in
  parallel independently; ideally they store the data that is
  being browsed locally.

The Hillview service is implemented in Java.  The UI is written in
TypeScript.

Hillview can run as a federated system of loosely interconnected
components; the root node only sends small queries to the workers, and
it receives small results from these.  The data on the workers is
never sent over the network; the worker locally compute all views that
are needed to produce the final result.

### 1.2. Streaming interaction

Users initiate operations in Hillview by performing operations within
the browser.  As a result of an operation, the back-end computes the
result as a series of approximations; these incremental results are
streamed back to the client and rendered, presenting the output with
gradually increasing precision.  Incremental results are always
accompanied by a progress bar, as in the following figure:

![Progress bar](progress.png)

Pressing the "stop" button next to a progress bar will cancel the
currently-executing operation.

### 1.3. Data model

The Hillview data model is a large table, with a relatively small
number of columns (currently tens or hundreds) and many rows (millions
to billions).

The data in each column is typed; Hillview supports the following data
types:
  * String (Unicode strings)
  * JSON (strings that represent JSON values)
  * Double (64-bit floating point)
  * Integer (32-bit)
  * Date+time (the Java Instant class is used to represent such date+time values
    on the server side; dates include time zone information)
  * Time intervals (represented using the Java Duration class)

Hillview supports a special value "missing" which indicates that a
value is not present.  This is similar to NULL values in databases.

## 2. Interacting with data

In this section we describe the various ways to present and interact
with the data.

### 2.1. Error display

Some operations can trigger errors.  For example, the attempt to load
a non-existent file.  These errors usually manifest as Java exceptions
in the backend.  Today the Hillview front-end captures these
exceptions and displays them on the screen.  We are working to improve
the usability of error messages.

![Error displayed by Hillview](exception.png)

### 2.2. Mouse-based selection

Several views allow the user to use the mouse to select data.
Selections can be modified using the keyboard as follows:

* Clicking on an element will select it and will unselect all other
  selected elements.

* Clicking while pressing shift button will select or unselect a whole
  range of contiguous elements.

* Clicking while pressing the control button
  will toggle the selection of the current element, while leaving the
  selection status of other elements unchanged.

### 2.3. Loading data

Hillview supports reading data from multiple data-sources.

When the program starts the user is presented with a Load menu.

![Load menu](load-data-menu.png)

The load menu allows the user to specify a dataset to load from
storage.

* Hillview logs: when this option is selected Hillview loads the logs
  produced by the Hillview system itself as a table with 9 columns.
  This is used to debug the Hillview system itself.

* Generic logs: allows the user to [read logs from a set of log
  files](#233-reading-generic-logs).

* Saved view: allows the user to [read data from a saved
  view](#234-reading-saved-data-views).

* CSV files: allows the user to [read data from a set of CSV
  files](#235-reading-csv-files).

* JSON files: allows the user to [read the data from a set of JSON
  files](#236-reading-json-files).

* Parquet files: allows the user to [read the data from a set of
  Parquet files](#239-reading-parquet-files).

* ORC files: allows the user to [read the data from a set of ORC
  files](#237-reading-orc-files).

* DB tables: allows the user to [read data from a set of federated
  databases using JDBC](#238-reading-data-from-sql-databases).

After the data loading is initiated the user will be presented with a
view of the loaded table.  If the table has relatively few columns,
the user is shown directly a [Tabular view](#32-table-views).  Otherwise
the user is shown a [Schema view](#31-schema-views), which can be
used to select a smaller set of columns to browse.

#### 2.3.1. Specifying the data schema

For some file formats that are not self-describing Hillview uses a
`schema` file to specify the format of the data.  The following is an
example of a schema specification in JSON for a table with 2 columns.
The "schema" file is stored on the worker nodes, in the same place
where the data resides.

```JSON
[{
    "name": "DayOfWeek",
    "kind": "Integer"
}, {
    "name": "FlightDate",
    "kind": "Date"
}]
```

The schema is an array of JSON objects each describing a column.  A
column description has two fields:

* name: A string describing the column name.  All column names in a
  schema must be unique.

* kind: A string describing the type of data in the column,
  corresponding to the types in the [data model](#13-data-model).  The
  kind is one of: "String", "JSON", "Double", "Integer",
  "Date", and "Interval".

#### 2.3.2. Specifying rules for parsing logs

We rely on the grok library for this purpose. For more info visit https://github.com/thekrakken/java-grok/blob/master/README.md

Grok works by combining text patterns into something that matches your logs.

The syntax for a grok pattern is %{SYNTAX:NAME}

* SYNTAX : The pattern that will match your text. For example, 3.44 will be matched
by the "NUMBER" pattern and 55.3.244.1 will be matched by the "IP" pattern

* NAME : An identifier naming the matched text. For example, 3.44 could be the duration
of an event, so you could call it simply "duration". Further, a string 55.3.244.1 might
identify the "client" making a request

For the above example, your grok pattern would look something like this:
```
%{NUMBER:duration} %{IP:client}
```
For more patterns : https://github.com/thekrakken/java-grok/blob/master/src/main/resources/patterns/patterns

##### 2.3.2.1. Example

Given this line from a syslog log:
```
Sep 17 06:55:14,123 pndademocloud-hadoop-dn-3 CRON[25907]: (CRON) info (No MTA installed, discarding output)
```

This line can be parsed with the following pattern:
```
%{SYSLOGTIMESTAMP:timestamp} (?:%{SYSLOGFACILITY} )?%{SYSLOGHOST:logsource} %{GREEDYDATA:message}
```

This creates the following log record.
```
logsource : pndademocloud-hadoop-dn-3
message   : CRON[25907]: (CRON) info (No MTA installed, discarding output)
timestamp : Sep 17 06:55:14.123
```

##### 2.3.2.2. Regular Expressions

Grok sits on top of regular expressions, so any regular expressions are valid in grok as well

##### 2.3.2.3. Custom Patterns

Hillview supports certain pre-defined log format patterns. You have an option to define custom log format patterns
to suit your needs and specify the same in the logFormat field in the UI (under Generic Logs). Given a log pattern
and a set of files, what you will get in Hillview is a table with columns corresponding to the names that you gave to the patterns.

#### 2.3.3. Reading generic logs

Hillview can read data from log files with diffrent log formats. The
following menu allows the users to specify the files to load.  *The
files must be resident on the worker machines where the Hillview service
is deployed*.

![Specifying log files](generic-log-menu.png)

* Folder: Folder containing the files to load.

* File name pattern: A shell expansion pattern that names the files to
  load.  Multiple files may be loaded on each machine.

* Log format: The [log format](#232-specifying-rules-for-parsing-logs) of the logs.

#### 2.3.4. Reading saved data views

Hillview can reload a view that was previously visualized. The
following menu allows the users to specify the files to load.

![Specifying a view to load](bookmarked-data-menu.png)

* File: A file containing the bookmarked data to load.

* Tab label: A name to display for dataset.

#### 2.3.5. Reading CSV files

Hillview can read data from comma- or tab-separated files. The
following menu allows the users to specify the files to load.  *The
files must be resident on the worker machines where the Hillview service
is deployed*.

![Specifying CSV files](csv-menu.png)

* Folder: Folder containing the files to load.

* File name pattern: A shell expansion pattern that names the files to
  load.  Multiple files may be loaded on each machine.

* Schema file: An optional [schema file](#231-specifying-the-data-schema)
  in JSON format that describes the schema of the data.  In the
  absence of a schema file Hillview attempts to guess the type of data
  in each column.  The schema file must reside in same folder.

* Header row: select this option if the first row in each CSV file is
  a header row; the first row is used to generate names for the
  columns in the absence of a schema.  If a schema is supplied the
  first row is just ignored.

All the CSV files must have the same schema (and the same number of
columns).  CSV files may be compressed (e.g., using gzip or other
compression tools).  CSV fields may be quoted using double quotes, and
then they may contain newlines.  An empty field (contained between two
consecutive commas, or between a comma and a newline) is translated to
a 'missing' data value.

#### 2.3.6. Reading JSON files

Hillview can read data from JSON files. The following menu allows the
users to specify the files to load.  *The files must be resident on
the worker where the Hillview service is deployed*.

![Specifying JSON files](json-menu.png)

* Folder: Folder containing the files to load.

* File name pattern: A shell expansion pattern that names the files to
  load.  Multiple files may be loaded on each machine.

The assumed format is as follows:
- the file contains a single JSON array
- the array elements are flat JSON objects
- each value will become a row in the table
- all JSON objects have the same structure (schema)
- JSON objects generate a column for each property

All the JSON files must have the same schema.  JSON files may be
compressed.

#### 2.3.7. Reading ORC files

Hillview can read data from [Apache ORC
files](https://github.com/apache/orc), a columnar storage format.
*The files must be resident on the worker machines where the Hillview
service is deployed*.  Hillview only supports files whose ORC schema
is an ORC struct with scalar types as fields.

![Specifying ORC files](orc-menu.png)

* Folder: Folder containing the files to load.

* File name pattern: A shell expansion pattern that names the files to
  load.  Multiple files may be loaded on each machine.

* Schema file: An optional [schema file](#231-specifying-the-data-schema)
  in JSON format that describes the schema of the data.  The schema
  file must reside in same folder, and it must be compatible with the
  ORC schema.

#### 2.3.8. Reading data from SQL databases

The following menu allows the user to load data from a set of
federated databases that are exposed as a JDBC service.  *Each worker
machine in the cluster will attempt to connect to the database
independently.* This works best when a separate database server is
deployed on each local Hillview machine hosting a worker.

Currently there is no way to load data from a single external database
when Hillview is deployed as a cloud service; however, data can be
loaded from a database when Hillview is deployed as a service running
on the local user machine.

The following menu allows the user to specify the data to load.

![Specifying database connections](db-menu.png)

* database kind: A drop-down menu indicating the kind of database to
  load data from.  Currently we support 'mysql' and 'impala'.

* host: The network name of a machine hosting the database.  *TODO*
  this should be a pattern enabling each worker to specify a different
  machine.

* port: The network port where the database service is listening.

* database: The database to load data from.

* table: The table to load data from.

* user: The name of the user connecting to the database.

* password: Credentials of the user connecting to the database.

Numeric values are converted either to integers (if they fit into
32-bits) or to doubles.  Boolean values are read as strings
containing two values, "true" and "false".

#### 2.3.9. Reading Parquet files

Hillview can read data from [Apache Parquet
files](http://parquet.apache.org), a columnar storage format.  The
[Impala](https://impala.apache.org/) database uses Parquet to store
data.  *The files must be resident on the worker machines where the
Hillview service is deployed*.

![Specifying Parquet files](parquet-menu.png)

* Folder: Folder containing the files to load.

* File name pattern: A shell expansion pattern that names the files to
  load.  Multiple files may be loaded on each machine.

Parquet Int96 data types are read as Datetime values.  Boolean values
are read as strings containing two values, "true" and "false"; byte
arrays are read as strings.  Hillview will reject Parquet files that
contain nested types (e.g., arrays).

### 2.4. Navigating multiple datasets

The Hillview user interface uses a tabbed web page to display multiple
datasets; each dataset is opened in a separate tab.  Even if the same
dataset is loaded a second time, it will be displayed in a new tab.
The current dataset is shown in a highlighted tab (white in the image
below).  The dataset name is shown in the tab; the user can click on
the tab to edit the displayed dataset name.

![Dataset tabs](dataset-tabs.png)

By clicking on a dataset tab the user will bring the dataset into
view.  A tab can be closed by clicking on the red x sign in the tab.
The currently-selected dataset is displayed below the line of tabs;
the page contains simultaneously multiple different views of a
dataset.

## 3. Data views

As the user navigates the dataset new views are opened and displayed.
This image shows a browser window containing multiple views of the
same dataset; three views are visible in the browser window, and the
user needs to scroll up and down to see the views.

![User interface organization](ui-organization.png)

Each view has a heading that describes it briefly, as shown in the
following figure.  Each view has a unique number, shown before the
title.  The lineage of views is usually shown in the title, allowing
users to navigate from a view to the source view from which it was
generated.  Views can also be closed by clicking the button marked
with x.

![View heading](view-heading.png)

The two black triangles in the view heading allow the view to be moved
up and down on the web page.  The axis markers X, Y, and G can be
dragged between some views; dropping them will "move" the
corresponding axis from the source view to the target view.  (G stands
for the "group-by" axis in a Trellis plot view.)

### 3.1. Schema views

The data schema views allow users to browse the schema of the current
table and select a set of columns from the dataset to focus on. This
feature is especially useful when the table contains too many columns
to display at once, and the user wants to focus on a subset of them.

The following example shows a schema view; the rows in a schema view
are the description of the columns of the data table.  In this example
there are three rows selected.

![Schema view](schema-view.png)

The schema view allows users to view the columns of the dataset and to
select a subset of columns to browse in detail.  The schema view has a
menu with the following options:

![Schema menu](schema-menu.png)

* Save as: allows the user to [save a copy of the data](#314-saving-data)
  in a different format; *the data is saved on the cluster where the
  service is running*

* View: allows the user to [change the way data is
  displayed](#312-the-schema-view-menu)

* Chart: allows users to [draw charts](#313-the-chart-menu) of one or two
  colums

* Combine: allows users to [combine data in two
  views](#44-combining-two-views)

#### 3.1.1. Selecting columns

There are two ways to modify the selection:
1. By [using the mouse](#22-mouse-based-selection).

2. Using the selection menus, which can be accessed either by
right-clicking on the **Name**, **Type** or **Allows Missing** column
headers, or by clicking on the **Select** menu option at the top left
as shown below.

![Column selection menu](schema-select-menu.png)

Columns can be un/selected using either the name or type.  We describe
the search criteria allowed in detail below.  In all cases, the search
returns a subset of column descriptions, which can be added to or
removed from the current selection.

* By Name: allows regular expression matching against the name of the column.

![Select by name Menu](name-selection.png)

* By Type: allows choosing all columns of a particular type.

![Select by type Menu](type-selection.png)

Once the user selects a set of column descriptions, they can display a view of the
data table restricted to the selected columns using the View/Selected columns menu.
Right-clicking on a selected set of descriptions opens up a context menu

![Context menu for selected rows in a schema view](schema-view-context-menu.png)

The following operations can be invoked through the context menu:
* Show as table: this displays a [table view](#32-table-views) of the
  data restricted to the selected columns.

* Histogram: draws a [1D](#34-uni-dimensional-histogram-views) or
  [2D](#35-two-dimensional-histogram-views) histogram of the selected
  columns

* Heatmap: draws a [heatmap](#36-heatmap-views) view of the selected columns.

* Trellis histogram: draw the selected columns using a Trellis view of
  [1D](#41-trellis-plots-of-1d-histograms) or
  [2D](#42-trellis-plots-of-2d-histograms) histograms

* Trellis heatmaps: draw the selected columns using a [Trellis
  view](#43-trellis-plots-of-heatmaps) of heatmaps

* Estimate distinct elements: estimate the number of distinct values in this column

* Filter...: opens up a filter menu that allows the user to filter data based on values in the selected column.
  See the description of the filter operation [below](#323-operations-on-selected-columns).

* Compare...: compares the data in the column with a specified constant.
  See the description of the compare operation [below](#323-operations-on-selected-columns).

* Create column in JS...:
  See the description of the create column operation [below](#323-operations-on-selected-columns).

* Rename...: shows up a menu that allows the user to rename this column

* Frequent elements...: shows up a menu that allows the user to find frequent elements
  See the description of the frequent elements operation [below](#323-operations-on-selected-columns).

* Basic statistics: shows for each selected column some basic statistics, as in the following figure:

  ![Basic statistics](basic-statistics.png)

#### 3.1.2. The schema view menu

![Schema view menu](schema-view-menu.png)

* Selected columns: this displays a [table view](#32-table-views) of the
data restricted to the selected columns.

#### 3.1.3. The chart menu

The user can also directly draw a chart of the data in a selected set
of columns using the chart menu:

![Chart menu](chart-menu.png)

* 1D Histogram...: presents a dialog allowing the user to
  select a column whose data will be drawn as a
  [uni-dimensional histogram view](#34-uni-dimensional-histogram-views).

![1D histogram dialog](1d-histogram-dialog.png)

* 2D Histogram...: presents a dialog allowing the
  user to select two columns whose data will be drawn as a
  [two-dimensional histogram view](#35-two-dimensional-histogram-views).

![2D histogram dialog](2d-histogram-dialog.png)

* Heatmap...:  presents a dialog allowing the user to
  select two columns whose data will be drawn as a [heatmap](#36-heatmap-views).

![Heatmap dialog](heatmap-dialog.png)

* Trellis histograms...: presents a dialog allowing the user to
  select two columns to use to display a [Trellis histogram view](#41-trellis-plots-of-1d-histograms).

* Trellis 2D histograms...: presents a dialog allowing the user to select three columns
  to use to display a [Trellis 2D histogram view](#42-trellis-plots-of-2d-histograms).

* Trellis heatmaps...: presents a dialog allowing the user to select three columns to use
  to display a [Trellish plot of heatmaps](#43-trellis-plots-of-heatmaps).

#### 3.1.4. Saving data

* This menu allows users to save the data in a different format as
  files on the worker machines.

![Save-as menu](saveas-menu.png)

* Save as ORC files: allows users to specify how data should be saved
  in the [ORC file format](#237-reading-orc-files).

![Save-as ORC menu](saveas-orc-menu.png)

The user can specify a folder on the remote worker machines.  Each
file in the current dataset will be saved as a new file in ORC format
in the specified folder.

### 3.2. Table views

The next figure shows a typical table view.

![Table user-interface](table-ui.png)

The columns in a table view are colored as follows:

* Columns that are hidden are shown in gray

* Columns that are visible are shown in white

* Columns that contain metadata are shown in green.  Metadata columns
  do not actually exist in the dataset, they are computed only when
  displaying a view.  For example, the second column always shows the
  count of rows that have a specific value.  Metadata columns do not
  offer all operations that are available on data columns.

Double-clicking on a column separator will enlarge the column to the
left of the mouse to fit the displayed data.

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

This display is equivalent to the output of the following SQL query:

```SQL
SELECT COUNT(*), Sum(Distance), DayOfWeek, UniqueCarrier FROM data
GROUP BY DayOfWeek, UniqueCarrier
ORDER BY dayOfWeek ASC, UniqueCarrier ASC
LIMIT 20
```

Initially a table view displays no columns.  The user can choose which
columns are displayed or hidden.

Missing values are shown with a different color.  When sorting missing
values are considered larger than any other value.

![Missing value display](missing-value.png)

In the tabular display a visible row can correspond to multiple rows
in the original dataset.  For example, in this figure, the first
displayed row corresponds to 22.73 thousands different rows in the
dataset.  Otherwise said, if one ignores all columns except the 2
visible ones, there are 22.73 thousand rows that have these exact
values in the 2 visible columns (1/SFO).  This is displayed in the
first two columns of the table:

![Position information columns](table-position.png)

The first column, labeled (position) indicates with a horizontal
bar where in the sorted order the current displayed row
resides.  In this figure the first row is at the beginning of the
sorted order (the dark bar is at the very left).  The second column,
labeled (count) indicates how many rows in the dataset correspond to
the displayed row.  The horizontal bar also indicates what percentage of
the whole dataset is covered by this.  You can see that, although the
table only has 20 rows, it actually displays information for 212 thousand
rows in the original dataset, or 24% of the data!

#### 3.2.1. Scrolling

Because each displayed row summarizes information from multiple rows,
scrolling through a Hillview table is somewhat different from the
standard scrolling.  The scroll-bar image and interaction reflect
these differences.  The following image is a blow-up of the scroll bar
of the table above.

![Scrollbar](scrollbar.png)

The "visible region" of the scroll-bar size depicts the amount of
information displayed.  In the previous figure the visible region is
about 1/4 of the scroll-bar, this indicates that the data displayed
covers 1/4 of the rows in the dataset.

The scroll-bar can be moved using the keyboard (page up, page down,
home and end), or dragged using the mouse.  When moving the scroll-bar
the size of the "visible region" can change, sometimes dramatically,
depending on the distribution of the values in the visible columns.

To drag the scroll-bar with the mouse one has to grab the narrow
scroll-bar handle which is at the middle of the visible region.
Dragging the scroll-bar allows the user to specify a *quantile* in the
sorted data-set.  For example, if the user drags the handle to the
middle of the scroll-bar, this indicates that the user wants to know
the rows around the *median* of the distribution.  Hillview will bring
into view a set of rows that includes the requested quantile.

#### 3.2.2. Selecting columns

The user can [select](#22-mouse-based-selection) one or more column using
the mouse.  The figure above shows table with 3 selected columns.

#### 3.2.3. Operations on selected columns

Double-clicking on a column header will show or hide the data in that
column.

Right-clicking on a column header pops up a menu that offers a set of
operations on the currently selected columns, as shown in the
following figure:

![Right-click menu for columns](column-right-click.png)

The contents of this menu may depend on the type of the column and on
the current state of the display.

* Show: the selected columns will be added to end of the current
  sorting order and the columns will become visible.

* Hide: the selected columns will be removed from the sorting order.

* Drop: the selected column will be removed from the set of displayed
  columns.  There is no operation to bring back the column once it has
  been dropped from a view.  Note that the column is only dropped from
  the *current* views; other views that are displaying the column will
  continue to display it.

* Estimate distinct elements: selecting this option will run a
  computation that estimates the number of distinct values that exist
  in the selected column.  The shown number is just an approximation,
  but it should be a good approximation.

* Sort ascending: The selected columns will be moved to the front of
  the sort order in ascending order.

* Sort descending: The selected columns will be moved to the front of
  the sort order in descending order.

* Histogram: this option requires exactly one or two columns to be
  selected.  If one column is selected, this operation will draw a
  histogram of the data in the selected column.  See
  [Uni dimensional histogram views](#34-uni-dimensional-histogram-views).
  If two columns are selected this menu will draw a two-dimensional
  histogram of the data in the selected columns.  For two-dimensional
  histograms see [Two-dimensional
  histograms](#35-two-dimensional-histogram-views).

* Heatmap: this option requires exactly two columns to be selected.
  This displays the data in these columns as a [Heatmap
  view](#36-heatmap-views).

* Trellis histograms: this option requires exactly two or three
  columns to be selected.  If two columns are selected, this operation
  will draw a trellis plot of 1-dimensional histogram of the data in
  the first selected column grouped by the second column.  See
  [Trellis plots of 1D histograms](#41-trellis-plots-of-1d-histograms).
  If two columns are selected this menu will draw a two-dimensional
  histogram of the data in the selected columns.  For two-dimensional
  histograms see [Two-dimensional
  histograms](#35-two-dimensional-histogram-views).

* Trellis heatmaps: this options requires exactly 3 columns to be
  selected.  This displays the data as a [Trellis plot
  view](#43-trellis-plots-of-heatmaps).

* Rename...: this operation requires exactly one column to be selected.
  The user can type a new name for this column.  The new name will be
  used for this column.  Note that other views that are already
  displaying the column will continue to use the old name for this
  column.

* Frequent elements...: finds the most frequent values that appear in
  the selected columns.  The user is presented with a dialog
  requesting the threshold parameter for the frequent elements
  computation.

  ![Frequent elements menu](heavy-hitters-menu.png)

  The user has to specify a percentage, between .01 (1/10,000 of the
  data) and 100 (the whole data).  The result is all items whose
  frequency in the selected columns is above the threshold. the result
  is shown in a [frequent elements view](#33-frequent-elements-views).

* PCA...: principal component analysis.  [Principal Component
  Analysis](https://en.wikipedia.org/wiki/Principal_component_analysis)
  is a method to project data in a high-dimensional space to a
  lower-dimensional space while preserving as much of the "shape" of
  the data.  The user must have selected a set of columns containing
  numerical data.  The number of columns is the original dimension of
  the data.

  ![PCA menu](pca-menu.png)

  The user must indicate the number of dimensions for the projection,
  which has to be smaller than the original number of columns.  The
  PCA analysis will append a set of numeric columns to the dataset,
  containing the result of the PCA analysis.  The name of each
  appended column will indicate the amount of variance in the original
  data that is captured by the column (0-100%).

* Plot singular value spectrum.  This operation requires a set of
  numeric columns.  This will display the singular values of the matrix formed
  from these columns as a [Singular value spectrum view](#37-singular-value-spectrum-views).

* Filter...: this option will pop-up a dialog window that allows the user
  to filter the data in the selected column (this option requires only
  one column to be selected). The user enters a search pattern. There
  is a checkbox which when selected, will interpret the pattern as a
  [Java regular
  expression](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html). There
  is a second checkbox which allows the user to choose  whether the
  matching values are to be kept or discarded.

  ![Filter menu](filter-menu.png)

* Compare...: compares the data in a column with a specified constant.
  Only one column must be selected.

  ![Compare menu](compare-menu.png)

  The comparison will apply a filter, keeping only the rows that
  satisfy the condition.  In this image we show a comparison which
  keeps all rows where the ArrDelay column has the value 10.  For
  string values the comparison is done alphabetically.

* Convert...: convert the type of data in a column.  Only one column
  must be selected.

  ![Convert menu](convert-menu.png)

  The conversion allows the user to change the type of data in a
  column.  For example, a numeric column can be converted to a string
  column.  After conversion a new column is appended to the table, containing the
  converted data.

* Aggregate...: this pops up a menu that allows the user to create a metadata
  column using an aggregation function over the data in the selected column.

  ![Aggregate menu](aggregate-menu.png)

* Create column in JS...: allows the user to write a JavaScript program that
  computes values for a new column.

  ![Add column dialog](add-column-dialog.png)

  The user has to specify the new column name and type.  The
  JavaScript program is a function called 'map' that has a single
  argument called 'row'.  This function is invoked for every row of
  the dataset.  'row' is a JavaScript map that can be indexed with a
  string column name.  For example, the following program extracts the
  first letter from the values in a column named 'OriginState':

```
function map(row) {
   var v = row['OriginState'];
   if (v == null) return null;
   v = v.toString();
   if (v.length == 0) return "";
   return v[0];
}
```

* Extract value...: this option is only available when browsing data
  that originates from some log files, where the current column format
  is of the form key=value pairs.  This option allows the user to
  specify a key and a destination column; the associated value will be
  extracted into the indicated column.

#### 3.2.4. Operations on a table cell

The user can also right-click on a cell in a visible column to pop-up
a menu allowing filtering based on values; the menu permits to
keep all rows based on a comparison with the specific table cell:

![Cell filtering context menu](filter-context-menu.png)

#### 3.2.5. Operations on tables

The table view has a menu that offers the following options:

![Table menu](table-menu.png)

* Save as: allows the user to [save a copy of the data](#314-saving-data)
  in a different format; *the data is saved on the cluster where the
  service is running*

* View: allows the user to [change the way the data is
  displayed](#326-the-table-view-menu).

* Chart: [allows the user to chart the data](#313-the-chart-menu).

* Filter: allows the user [to filter data](#327-the-table-filter-menu) according
  to various criteria.

* Combine: allows the user to [combine the data in two views](#44-combining-two-views).

#### 3.2.6. The table view menu

The view menu offers the following options.

![View menu](table-view-menu.png)

* Refresh: this redraws the current view of the table.

* No columns: all columns will be hidden.

* Schema: displays [the table schema](#31-schema-views).

* Change table size: this changes the number of rows displayed.

#### 3.2.7. The table filter menu

The table filter menu allows the user to find specific values or
filter the data according to specific criteria.

![Table-filter-menu](table-filter-menu.png)

* Filter data based on patterns that occur.  The user must specify a
  column, a string to find, and indicate whether the string is a
  regular expression and whether the filter keeps or excludes matching
  values.

![The table filter submenu](table-filter-submenu.png)

* Filter data in a specific column based on type-specific comparisons.
  The user must specify the column, the value sought, and a comparison
  relation.

![Table compare menu](table-compare-menu.png)

* Find rows that contain a specific substring.  The find will scroll
  the table view to the next row that contains the string sought.  The
  search is only done in the currently visible columns.  The user
  specifies a string to search, and whether the search matches on
  substrings, whether it treats the string as a regular expression,
  and whether it is case-sensitive.  Finding will be soon be enhanced
  with a "find next/find previous" menu, which allows the search to be
  quickly repeated.

![Table find menu](table-find-menu.png)

### 3.3. Frequent elements views

A frequent elements view shows the most frequent values that appear in the
dataset in a set of selected columns (above a certain user-specified
threshold).

![Frequent elements view](heavy-hitters-view.png)

The data is sorted in decreasing order of frequency.  Each row
displays a combination of values and its count and relative frequency
within the dataset.  A special value that may appear is "Everything else",
which indicates the total over all rows corresponding to elements  that do not appear
frequently enough individually to be above the chosen threshold. This value only appears
if the total over all these rows is itself above the threshold.

There are two menu options offered from this view: [View as a table](#331-view-as-a-table) and [Modify](#332-modify).

#### 3.3.1. View as a Table

Clicking this button gives the user two options:
![View as a Table](heavy-hitters-tableMenu.png)

* All frequent elements as table: switches back to a [table
  view](#32-table-views), but where the table only contains the rows
  corresponding to the frequent values.

* Selected frequent elements As table: switches back to a [table
  view](#32-table-views), but where the table only contains the rows
  corresponding to the frequent values currently selected.

#### 3.3.2. Modify
Clicking this button gives the user two options:
![Modify](heavy-hitters-modifyMenu.png)
* Get exact counts: runs a more expensive but more precise
  frequent elements computation which computes the exact frequency for
  each value. This operation replaces the approximate counts in the display
  by the precise ones.

* Change the threshold: Recall the the user specifies a frequency threshold above
which elements are considered to be frequent. Clicking this menu pops up a dialog
box like the one shown below that allows the user to modify the threshold. This can
be useful to see a larger list for instance.

![Change Threshold Dialog](heavy-hitters-changeThreshold.png)

Note that if the threshold is set very low, then the number of results can be very large. HillView
only displays the 200 most frequent elements results, and alerts the user to the possible existence
of further frequent elements. These can be viewed using the All frequent elements option from the
 [View as a table](#331-view-as-a-table) menu option.

### 3.4. Uni-dimensional histogram views

A uni-dimensional (1D) histogram is a succinct representation of the
data in a column.  See [below](#344-string-histograms) for a
description of string histograms.  A histogram is computed in two
phases:

- first the range of the data in the column is computed (minimum and
  maximum values).

- the range is divided into a small number of equal buckets.  Then the
  data is scanned and the number of points in the column that fall in
  each bucket is computed.

A histogram is displayed as a bar chart, with one bar per bucket.  The
height of the bucket shows the number of values that fall within the
bucket.  The X axis corresponds to the column being plotted, and the Y
axis is the count of values within each bucket.  Histograms can be
computed only approximately, but in this case the error in each bar
should be smaller than one pixel in size.

The total number of missing values is displayed below the histogram
itself, as a number.

![A one dimensional histogram](hillview-histogram.png)

A number on top of each bar indicates the size of the bar.  If the size is only
approximately the value is shown with an approximation sign: &asymp;.

The thin blue line shown is the cumulative distribution function
([CDF](https://en.wikipedia.org/wiki/Cumulative_distribution_function)).
The CDF is drawn on a different implicit vertical scale ranging
between 0 and 100%.  A point on the CDF curve at coordinate X shows
how many of the data points in the displayed column are smaller than X.

Next to the mouse an overlay box displays the following values:
* the mouse's position on the X axis
* the mouse's position on the Y axis
* the size of the histogram bar at the current X coordinate
* the value of the CDF function at the current X coordinate of the mouse, in percents

#### 3.4.1. The histogram menu

Histogram views have a menu that offers to the users several operations:

![Histogram menu](histogram-menu.png)

* Export: exporting the data in the view.  Exporting the data creates
  a file named `histogram.csv` in the browser's download folder.  This
  file has two columns: one describing each X axis bucket labels and a
  second describing the bucket size.  This data can be used to plot
  the same histogram using a tool like Excel.

* View: [changing parameters](#342-the-histogram-view-menu) of the current view.

* Combine: [combining the data](#44-combining-two-views) in the current view with
  another one.

#### 3.4.2. The histogram view menu

![View menu](histogram-view-menu.png)

The "View" menu from a histogram display has the following functions:

* refresh: redraws the current histogram.

* table: switches to a tabular display of the data shown in the
  current histogram.

* exact: (only shown if the current histogram is only approximately
  computed) redraws the current histogram display by computing
  precisely the histogram.

* \#buckets: shows a menu that allows the user to specify the number
  of buckets; the histogram will be redrawn using the specified number
  of buckets.

* group by: select a second column and draw a [Trellis plot of a
  series of histograms](#41-trellis-plots-of-1d-histograms) with the data
  grouped on the values in the second column

* correlate: allows the user to specify a second column and switches
  the display to a [two-dimensional
  histogram](#35-two-dimensional-histogram-views)

#### 3.4.3. Mouse selection in histogram views

The mouse can be used to select a portion of the data in a histogram.
The user can click-and-drag to select a subset of the data.  The
selection chooses a contiguous range on the X axis.

![Selection in histogram](histogram-selection.png)

When the user releases the mouse button the selection takes place.
The selection can be cancelled by pressing the ESC key.  The selection
can be complemented by pressing the CONTROL at the time the selection
is released (this will eliminate all the data that has been
selected).

#### 3.4.4. String histograms

When drawing a histogram of string data it is possible to have
more values on the X axis than there are buckets.  In this case
each bucket will contain multiple string values, and one needs to
zoom-in by selecting to reveal the finer-grained structure of the data
in a bucket.  This case can be distinguished visually since there will
be multiple ticks on the X axis for each bucket.  The CDF function
will also probably indicate that the data has a higher resolution than
shown by the buckets alone.

![String histogram](categorical-histogram.png)

The figure above shows a histogram where there are 294 distinct values
but only 40 buckets.  One can see multiple ticks in each bucket.  Only
some of the tick labels are displayed.

### 3.5. Two-dimensional histogram views

A 2D histogram is a useful visual tool for estimating whether the
values in two columns are independent of each other.  Neither of the
two columns can be a String column.  A 2D histogram is a 1D histogram
where each bucket is further divided into sub-buckets according to the
distribution in a second column.

For example, in the following figure we see a 2D histogram where the X
axis has the airline carriers.  For each carrier the bar is divided
into sub-bars, each of which corresponding to a range of departure
delays.  The color legend at the top shows the encoding of values into
colors.

![A two-dimensional histogram](hillview-histogram2d.png)

The thin blue line shown is the cumulative distribution function
([CDF](https://en.wikipedia.org/wiki/Cumulative_distribution_function)).
The CDF is drawn on a different implicit vertical scale ranging
between 0 and 100%.  A point on the CDF curve at coordinate X shows
how many of the data points in the displayed column are smaller than X.

For each bucket a transparent rectangle at the top of the bucket is
used to represent the missing data.

By default a histogram is computed over sampled data. The sampling
rate is presented at the bottom.

Next to the mouse an overlay box displays information about the mouse
position.  Consider the example below, which shows the distribution of
departure delays for each state (states are on the X axis).

![Histogram overlay information](histogram-2d-overlay.png)

The 6 rows represent the following values:

* DestState=NY indicates that the the mouse's X coordinate is at the NY value

* DepDelay=[-12,2) indicates that the pink color represents a range
  range of values corresponding to the values -12 to 2.

* y = 2,064,627 indicates the mouse's position on the Y axis

* count = 3,551,093 indicates the size of the pink bar, i.e., the
  number of flights in NY that have delays between -12 and 2.

* % = 64.14% indicates that delays between -12 and 2 represent roughly
    65% of all delays from NY

* cdf = 71.1%: indicates the value of the CDF function at the current
  mouse X coordinate, shown by the blue little dot; in other words,
  the flights from states alphabetically before NY make up 71.1% of the
  flights.

The "view" menu for a 2D histogram offers the following operations:

![View menu](2d-histogram-view-menu.png)

* Refresh: this causes the histogram to be re-displayed.

* Table: Displays a tabular view of the data visible in the current
  histogram.

* Exact: Computes the histogram bar heights exactly.

* \#buckets: Allows the user to choose the number of buckets to use
  for the X axis.

* swap axes: Draws a new 2D histogram where the two columns are
  swapped.

* heatmap: Displays a [heat map](#36-heatmap-views) of the data using the
  same two columns as in the current histogram.

* relative/absolute: This toggles between displaying the 2D histogram
  bars with relative sizes or normalized all to 100% height, as in the
  following image.

* group by: choose a third column and group the data into a set of 2D
  histograms displayed as a [Trellis plot of 2D
  histograms](#42-trellis-plots-of-2d-histograms).

![A normalized two-dimensional histogram](hillview-histogram-normalized.png)

For a description of the combine menu see [combining two views](#44-combining-two-views).

#### 3.5.1. Selection in 2D histograms

In a 2D histogram users can select data in two ways:

* X-axis based selection: similar to 1D histograms, users can select a
  range on the X axis to zoom into.  Similar with 1D histogram
  selection, by keeping the Control button pressed when the mouse
  button is released the user will select the complement of the
  specified range.

* Colormap based selection: the user can select a range in the
  colormap to perform a selection of the data based on the second
  column, as shown in the following image.  Note that the legend could
  contain a color for "missing" data, which cannot be selected.

![Selecting from a 2D histogram legend](legend-selection.png)

### 3.6. Heatmap views

A heatmap view displays the data in two columns.  Neither of the two
columns can be a string column.  The two columns are mapped to the two
axes of a plot, then the screen is divided into small patches a few
pixels wide and the number of data points that falls within each patch
is counted.  The number of values that falls within each patch is
displayed as a heatmap, where the color intensity indicates the number
of points. The mapping between the color intensity and the count could
be in log scale if the range is large or linear if the range is
smaller. Next to the mouse an overlay box displays the x value, the y
value and the count. A heatmap where neither axis is string
will also display a line that gives the best [linear
regression](https://en.wikipedia.org/wiki/Linear_regression).  between
the values in the two columns.

*TODO* discuss missing values.

![A heatmap](hillview-heatmap.png)

The colormap of a heatmap can be adjusted using a drop-down menu that
appears when right-clicking the colormap. The drop down menu also
allows to togle between a log scale color scheme and a linear color
scheme.

![Colormap menu](colormap-menu.png)

The heatmap view menu has the following operations:

![View menu](heatmap-view-menu.png)

* refresh: Redraws the heatmap

* swap axes: Swaps the X and Y axes.

* table: Displays the data in the current heatmap in a tabular form.

* Histogram: Draws a [2D histogram](#35-two-dimensional-histogram-views)
  of the data in the two columns that are used for the heatmap
  display.

* group by: Groups data by a third column creating a [Trellis plot]
  (#43-trellis-plots-of-heatmaps).

For a description of the combine menu see [combining two views](#44-combining-two-views).

#### 3.6.1. Selection from a heatmap

Users can select a rectangular area from a heatmap with the mouse.

![Selection from a heatmap](heatmap-selection.png)

### 3.7. Singular value spectrum views

The view display the [singular-value
decomposition](https://en.wikipedia.org/wiki/Singular-value_decomposition)
of a matrix composed of a set of numeric columns.  The singular values
are plotted as a bar-chart, as in the following figure:

![Singular value spectrum](singular-value-spectrum.png)

The user can inspect the displayed spectrum of singular values and
choose how many values to use in a principal component analysis
(PCA), as described below.  In the picture above the first 3
singular values explain most of the variance of the data.

## 4. Trellis plots

Hillview can display multiple histograms or heatmaps in a grid view
called a Trellis plot.  Each plot corresponds to a contiguous range of
values from a column.  For example, the figure below shows a Trellis
plot of histograms of the arrival delay, where each histogram is drawn
for a different state.

Selection in a Trellis plot can be done in several ways:
* dragging the mouse within a single plot will perform selection on
  the X axis of that plot.

![Selection within a single plot](trellis-simple-selection.png)

* dragging the mouse across multiple plots will select the data
  corresponding the all plots congiguously between the first one
  selected and the last one selected

![Selection across multiple plots](trellis-complex-selection.png)

### 4.1. Trellis plots of 1D histograms

Hillview can display multiple histograms in a grid, using a so-called
Trellis plot view.

![A Trellis plot of 1D histograms](hillview-histogram-array.png)

The following operations are available from the View menu of a Trellis histogram view:

![View operations on Trellis histograms](trellis-histogram-view-menu.png)

* refresh: will redraw the view
* table: will display the underlying data in a [tabular view](#32-table-views).
* exact: will compute and display the histograms without approximation
* \#buckets: allows the user to change the number of buckets displayed for each histogram
* \#groups: allows the user to change the number of groups used for displaying the Trellis plot
* correlate...: allows the user to specify a second column that is used for displaying a Trellis plot
  of 2D histograms

### 4.2. Trellis plots of 2D histograms

*Currently not yet implemented*

### 4.3. Trellis plots of heatmaps

![A Trellis plot of heatmap views](hillview-heatmap-array.png)

A Trellis plot is an array of heatmaps, grouped by a third column.
Each heatmap displays data that corresponds to a range of values in
the grouping column.

### 4.4. Combining two views

Any view represents logically a subset of rows from an original table.
Two different views can be combined by performing a set operation
between the rows that they represent.  This is done using the "Combine"
menu, which is present in almost all views.

![Combining two views](combine-menu.png)

The operations are as follows:

* Select current: this selects the current view for a subsequent
  combining operation.

* Union: combines the previously-selected view with the current one by
  keeping all rows that appear in either view.

* Intersection: combines the previously-selected view with the current
  one by keeping only the rows that appear in both views.

* Exclude: combines the previously-selected view the current one by
  excluding from the current view all the rows from the previous one.

* Replace: replaces the data in the current view with the data from
  the previously-selected one.
