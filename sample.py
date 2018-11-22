myGlobal = 5

def func1():
    global myGlobal
    myGlobal = 32

def func2():
    print myGlobal

func1()
func2()
