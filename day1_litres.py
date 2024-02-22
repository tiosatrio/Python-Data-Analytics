# 1 hours is 0,5 litres
time = 1
litre = 0.5
time_cycling = 3
def litres(time):
    if time == 0:
        times = 0
    else:
        times=round((time_cycling)*(litre)/(time))
    comm= str(time_cycling)+' hours cycling'+' need drink '+ str(times) + ' litres of warter'
    return comm

print(litres(time))

