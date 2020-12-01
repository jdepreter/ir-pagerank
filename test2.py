import pandas
import numpy as np

links = pandas.read_csv("ClueWeb09_WG_50m.graph-txt", sep="\n", header=None, nrows=5, skip_blank_lines=False)
links['index'] = np.arange(links.shape[0])
print(links.values)