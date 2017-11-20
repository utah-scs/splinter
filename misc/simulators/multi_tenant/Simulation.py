'''
Created on Oct 28, 2017

@author: Manasa
'''
import matplotlib.pyplot as plt
import math

paramsMap = {}
class Data:
    y = 0 #tps
    params = [] #params(0,1,2) (0-tenants, 1-ps, 2-cs)
    distribution = ""
    def __init__(self, ps, cs, tenants,tps, dist):
        self.y = float(tps)
        p = []
        p.append(int(tenants))
        p.append(int(int(ps)/1000)) 
        p.append(int(int(cs)/1000)) 
        self.params = p
        self.distribution = dist   
    

class sort:
    x = 0
    y = 0
    z = 0
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z


def compare(obj1, obj2):
    if(obj1.x < obj2.x):
        return -1
    elif (obj1.x > obj2.x):
        return 1
    else:
        return 0

'''
dataArray: contains a list of all Data objects
distribution: is either uniform or zipf
x: 0 for tenants, 1 for ps, 2 for cs
fixed: 0 for tenants, 1 for ps, 2 for cs. This would remain constant for this plot.
multiple: 0 for tenants, 1 for ps, 2 for cs. This would represent multiple lines on the plot.       
'''
def plotGraph(dataArray, distribution, x, fixed, multiple):
    print("distribution "+distribution+" x "+str(x)+" fixed "+str(fixed)+" mult "+str(multiple))
    fixedSet = set()
    for dataObj in dataArray:
        if(dataObj.distribution == distribution):
            fixedSet.add(dataObj.params[fixed])
    multipleSet = set()
    for data in dataArray:
        if (data.distribution == distribution):
            multipleSet.add(data.params[multiple])
    for fix in fixedSet:
        #fig = 1
        for mul in multipleSet:
            array = []
            for d in dataArray:
                if(d.distribution == distribution and d.params[multiple] == mul and d.params[fixed] == fix):
                    sortObj = sort(d.params[x], d.y, mul)
                    array.append(sortObj)
            array.sort(compare)
            xVals = []
            yVals = []
            for ob in array:
                xVals.append(ob.x)
                yVals.append(ob.y)
            plt.plot(xVals, yVals, 'o-',label = str(mul))
            #plt.savefig(str(fig))
            #fig = fig + 1
        plt.xlabel(paramsMap[x]+" (Micro seconds)")
        plt.ylabel(" Throughput (Requests per second)")
        plt.title(str(distribution)+" distribution for a fixed "+paramsMap[fixed]+" "+str(fix)+" and different "+paramsMap[multiple])
        plt.legend()
        plt.show()       
    
if __name__ == '__main__':
    array = []
    for line in open("C:/Users/Chandhan/Desktop/DistributedSystems/IS/data").readlines():
        array.append(line)
    dataArray = []
    for index in range(len(array)):
        if index == 0:
            continue
        words = array[index].split()
        #print(words[0]+" "+words[1]+" "+words[2]+" "+words[3]+" "+words[4])
        
        data = Data(words[1], words[2], words[3], words[4], words[0])
        dataArray.append(data)
        #print("data is "+data.distribution+" "+str(data.params[1])+" "+str(data.params[2])+" "+str(data.params[0])+" "+str(data.y))
    paramsMap[0] = "Tenants"
    paramsMap[1] = "Processing Time"
    paramsMap[2] = "Context Switch Time"
    plotGraph(dataArray, "Zipf", 1, 0, 2)
   # plotGraph(dataArray, "Uniform", 0, 1, 2)
    
    
    