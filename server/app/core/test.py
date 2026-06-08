'''def compute(start, end):
    # Пример: интеграл 4 / (1 + x*x) на [0, 1].
    # TOTAL_ITERATIONS приходит от мастера и равно полю "Итераций всего".
    a = 0.0
    b = 1.0
    dx = (b - a) / TOTAL_ITERATIONS

    result = 0.0
    for i in range(start, end):
        x = a + (i + 0.5) * dx
        result += 4.0 / (1.0 + x * x) * dx
    return result'''

def compute(start, end):
    # Пример: сумма квадратов на отрезке [start, end).
    result = 0.0
    for i in range(start, end):
        result += i * i
    return result   