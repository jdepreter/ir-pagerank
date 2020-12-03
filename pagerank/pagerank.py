import numpy as np


M = np.matrix([
    [0.5, 0.5, 0],
    [0.5, 0,   1],
    [0,   0.5, 0]
])

length = 3
r = np.matrix([
    [1.0 / length],
    [1.0 / length],
    [1.0 / length]
])

print(r)
for i in range(50):
    r = np.dot(M, r)
    print(r)
