with open("a.csv") as f:
    s = f.readline()
    l = s.split(", ")
    for a in l:
        print(a)