import math

def min_happen_prop(N: int, M: int, p: float):
    """
    N次实验中，计算某事件最少发生M次的概率
    :param N: 总实验次数
    :param M: 事件最少发生次数
    :param p: 事件发生概率
    :return:
    """
    tp = 0
    for k in range(M, N + 1):
        c = math.factorial(N) / (math.factorial(k) * math.factorial(N - k))
        tp += c * math.pow(p, k) * math.pow(1 - p, N - k)
    return tp

