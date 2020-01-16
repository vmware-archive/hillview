# Hillview for differentially-private visualizations

We are experimentally adding support to Hillview for
[differentially-private](https://en.wikipedia.org/wiki/Differential_privacy) visualizations.
This is work in progress.

A [video](https://1drv.ms/v/s!AlywK8G1COQ_lJMxl9cantTOlvRawA?e=k1VKSL) of Hillview being used to visualize 
differentially-private data is available.

Visualizations preserve privacy by adding noise to the
results prior to the visualization.  In Hillview differentially-
private views have an epsilon sign on each page: ε.

## Missing functionality

Many Hillview visualizations are unavailable when using differential privacy.
In particular, the table view has very restricted functionality - one
cannot inherently browse individual rows of a dataset when
using differential privacy.

## Privacy budget

