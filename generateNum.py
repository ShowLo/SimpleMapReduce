# -*- coding: UTF-8 -*-

import numpy as np
import os

count = 100000000
number = np.random.rand(count)
with open('number.txt', 'w') as f:
    for n in number:
        f.write(str(n) + os.linesep)
