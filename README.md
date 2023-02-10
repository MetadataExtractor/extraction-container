# extraction-container
Containerized Metadata Extractor for eDNAExplorer


This script, which is designed to run within a container service, does the following:
1. Imports metadata associated with your environmental DNA (eDNA) samples.  This metadata, which needs to be formatted as a csv file, needs to contain information on the location and time of the samples, as well as the spatial uncertainty associated with the location.  [Here's an example of an input metadata file](https://drive.google.com/file/d/1eEwXMww7vYn6ia-gu3KIH1nf_YTgA6Es/view?usp=sharing).  Note that it contains a number of variables beyond those required.
2. Extracts a variety of environmental and social variables at the eDNA sampling locations.  The spatial extent of this extraction is defined by the median spatial uncertainty of the sample set, the temporal range is defined as +/- 6 months of the earliest and latest sampling date.
3. Exports extracted values to a csv file in the top level directory of your Google Drive account.  [Here's an example output file](https://drive.google.com/file/d/1Ld9v0VSzyWydbU4s0aYMK_qKwRVb1vK2/view?usp=sharing) generated using the required columns from the example metadata input file.

