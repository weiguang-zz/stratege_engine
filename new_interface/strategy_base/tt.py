

import random
import scipy.stats

a = [random.random() for i in range(100)]
b = [random.random() for i in range(100)]

new_a = []
for i in range(100):
    v = random.random()
    if i>0:
        v = new_a[i-1] * 0.5 + v * 0.5
    new_a.append(v)

new_b = []
for i in range(100):
    v = random.random()
    if i>0:
        v = new_a[i-1] * 0.5 + v * 0.5
    new_b.append(v)


print(scipy.stats.spearmanr(a, b))

print(scipy.stats.spearmanr(new_a, new_b))