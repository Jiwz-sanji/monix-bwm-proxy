
def hex2dec(num):
    if num == 0:
        print(0)

    while(num != 0):
        if  num%16<=9:
            A.append(num%16)
        else:
            if num%16==10:
                i = 'A'
            elif num % 16 == 11:
                i = 'B'
            elif num%16==12:
                i = 'C'
            elif num%16==13:
                i = 'D'
            elif num%16==14:
                i = 'E'
            elif num%16==15:
                i = 'F'
            A.append(i)
        num = num//16
    A.reverse()
    for j in  A:
        print(j,end='')


if __name__ == '__main__':
    A = []
    num = int(input())
    hex2dec(num)