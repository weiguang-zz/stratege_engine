
import random
import numpy as np
T = 100
h = 5
sigma = 0.02
a = []
for i in range(T):
    a.append(random.normalvariate(0, sigma))

# variance of variance
# h天累计回报的方差的理论值是 h * sigma * sigma
b = []
for i in range(100):
    p = random.randint(0, T-h-1)
    b.append(sum(a[p: p+h]))
print(np.var(b))

# h天累计回报的方差的方差的理论值 h * sigma * sigma * sigma * sigma
c = []
for i in range(1000):
    # vs = []
    # for j in range(100):
    sv = [random.normalvariate(0, sigma) for k in range(h)]
        # vs.append(sum(sv))
    c.append(np.power(sum(sv), 2))
print(np.var(c))

# 当使用重叠的样本数据后，得到的样本
d = []
for i in range(T-h+1):
    d.append(np.power(sum(a[i:i+h]), 2))
print(np.var(d))

print('done')



