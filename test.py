import datetime

if __name__ == '__main__':
    tuple_ = (2, 3, 'JPY', datetime.datetime(2020, 4, 1, 9, 17, 59), None)
    list = []
    for t in tuple_:
        if type(t) is datetime.datetime:
            list.append(t.strftime('"%Y-%m-%d %H:%M:%S"'))
        else:
            list.append(t)
    [print(a) for a in list]