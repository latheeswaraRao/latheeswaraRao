import pandas as pds
import numpy as np
import timeit
import time
import sys

list = range(1000)
print(sys.getsizeof(1)*len(list))
array = np.arange(1000)
print(array.size * array.itemsize)
df = pds.DataFrame(list)
print(df.info(memory_usage='deep'))





