# ir-pagerank

## Pyspark
Make sure you have the following installed: 
- Spark
- Java 
- pyspark (pip)
- findspark (pip)


To run the big dataset (not recommended).
First, prepare the dataset by adding line numbers and removing empty nodes.

> awk 'BEGIN{i=-1} /.*/{printf "%d.%s\n",i,$0; i++}' ClueWeb09_WG_50m.graph-txt > ClueWeb09_WG_50m_numbered.graph-txt \
> sed -r --in-place 's/[0-9]*\.$//g' ClueWeb09_WG_50m_numbered.graph-txt \
> sed -i '/^$/d' ClueWeb09_WG_50m_numbered.graph-txt

Then run the python script.

> cd pyspark \
> python3 pagerank.py 

variables like α, ε, input- and outputfiles can be changed by editing the variables at the top of pagerank.py.

To run the google dataset, run
> cd pyspark/google-graph \
> python3 google-ranking.py

variables like α, ε, input- and outputfiles can be changed by editing the variables at the top of google-ranking.py.


The two optimazations attemps are run in the same way. A new directory will be created containing the sorted csv file. Make sure this directory does not already exists, the program refuses to overwrite already written output.

The output csv file is sorted on decreasing pagerank.

## C++ 
The source code can be found in \textit{./c++}. Compile main.cpp and run the binary with the following cli arguments
> cd c++ \
> g++ main.cpp\
> ./a.out help    # gives cli explanation \
> ./a.out inputfile format outputfile algorithm [alpha] [iterations]

|||
|---|---|
| inputfile | path to web graph file e.g. *../data/ClueWeb09\_WG\_50m.graph-txt* |
| format | graph-txt or google |
| outputfile | path to outputfile e.g. *out-power-0.15.csv*|
| algorithm | power or random (optional, default = power)|
| alpha | 0.0 - 1.0 (optional, default = 0.15)|
| iterations | max amount of iterations to run (optional, default = -1 (unlimited))|

The csv file output of the C++ program is sorted on node id. To sort by pagerank use the following command on the outputted csv file.

> sort --field-separator=';' --reverse --key=2,2 -g out-power-0.15.csv > out-power-0.15-sorted.csv
