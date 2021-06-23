

# import random
# import scipy.stats
# P = 0
# Q = 0
# for k in range(500):
#     a = [random.random() for i in range(500)]
#     b = [random.random() for i in range(500)]
#
#     new_a = []
#     for i in range(500):
#         v = random.random()
#         if i>0:
#             v = new_a[i-1] * 0.9 + v * 0.1
#         new_a.append(v)
#
#     new_b = []
#     for i in range(500):
#         v = random.random()
#         if i>0:
#             v = new_b[i-1] * 0.9 + v * 0.1
#         new_b.append(v)
#
#
#     print(scipy.stats.spearmanr(a, b))
#
#     print(scipy.stats.spearmanr(new_a, new_b))
#     r, pval = scipy.stats.spearmanr(new_a, new_b)
#     if pval<0.05 and r<0:
#         P += 1
#     if pval<0.05 and r>0:
#         Q += 1
#     print("--------------------------")
#
# print("显著负相关数量:{}，显著正相关数量:{}".format(P, Q))

class MyError(Exception):
    pass

def b():
    raise RuntimeError("eeror")

def f():
    try:
        b()
    except Exception as e:
        raise MyError(e)

f()
