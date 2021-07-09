import netCDF4 as nc
import numpy as np
import numpy.ma as ma
from mpi4py import MPI
import sys

class cdfData:
    def __init__(self,ds):
        self.precip=ds['precip']


    def sumAndAverageNorth(self):
        result =np.empty(daymax)
        for i in range(daymax):
            sum=0
            size=len(self.precip[i,0:latmax//2,:].compressed())
            if (size>0):
                res=(np.sum(np.sum(self.precip[i,0:latmax//2,:],axis=1),axis=0)/size)
                result[i]=res
        return result

    def sumAndAverageSouth(self):
        result =np.empty(daymax)
        for i in range(daymax):
            sum=0
            size=len(self.precip[i,latmax//2+1:latmax,:].compressed())
            if (size>0):
                res=(np.sum(np.sum(self.precip[i,latmax//2+1:latmax,:],axis=1),axis=0)/size)
                result[i]=res
        return result


def processAction(rank,mystartyear,numyears):
    data=[]
    nh=[]
    sh=[]
    mylocalt=0.0

    for i in range(numyears):
        fn = PATH_PREFIX+PATH_SUFFIX+FILE_PREFIX+FILE_SUFFIX+str(mystartyear+i)+EXTENSION
        fn = str(fn)
        ds = nc.Dataset(fn)
        data.append(cdfData(ds))

        t1=MPI.Wtime()
        nh.append(data[i].sumAndAverageNorth())
        sh.append(data[i].sumAndAverageSouth())
        t2=MPI.Wtime()
        mylocalt+=(t2-t1)

    t1=MPI.Wtime()
    mynh = np.sum(nh,axis=0)/len(nh)
    mysh = np.sum(sh,axis=0)/len(sh)
    t2=MPI.Wtime()
    mylocalt+=(t2-t1)

    t1=MPI.Wtime()
    comm.Reduce(mynh,mmpday_nh, MPI.SUM,0)
    comm.Reduce(mysh,mmpday_sh, MPI.SUM,0)
    t2=MPI.Wtime()
    reducetime=(t2-t1)
    reduction = np.array(reducetime)
    timing = np.array(mylocalt)

    comm.Reduce(timing,timetaken,MPI.MAX,0)
    comm.Reduce(reduction,globalreducetime,MPI.MAX,0)

class Stats:
    def __init__(self,data):
        self.data=data
        self.outliers=[]
        self.median=0
        self.q1=0
        self.q3=0
        self.iqr=0

    def getStats(self):
        self.median=self.data[182]
        self.q1=0.5*(self.data[90]+self.data[91])
        self.q3=0.5*(self.data[273]+self.data[274])
        self.iqr=self.q3-self.q1
        self.outliers=[]
        for i in self.data:
            if (i<self.q1-1.5*self.iqr):
                self.outliers.append(i)
            elif (i>self.q3+1.5*self.iqr):
                self.outliers.append(i)
    def dispStats(self):
        print(f"Median: {self.median}")
        print(f"Q1: {self.q1}")
        print(f"Q3: {self.q3}")
        print(f"IQR: {self.iqr}")
        print(f"Outliers: {self.outliers}")
        print(f"Total Exec Time: {timetaken}")
        print(f"Reduce Time: {globalreducetime}") 


PATH_PREFIX="/data/full_data_daily_"
FILE_PREFIX='/full_data_daily_'
EXTENSION=".nc"
latmax=180
lonmax=360
daymax=365
mmpday_nh=np.zeros(daymax)
mmpday_sh=np.zeros(daymax)
timetaken=np.array(0.0)
globalreducetime=np.array(0.0)

startyear=0
endyear=0
startyear=int(sys.argv[1])
if (sys.argv[3]=='2018'):
    PATH_SUFFIX='V2018_05'
    FILE_SUFFIX='v2018_05_'
    latmax=360
else:
    PATH_SUFFIX='v2020'
    FILE_SUFFIX='v2020_10_'


    
if(len(sys.argv)==2):
    endyear=startyear
else:
    endyear=int(sys.argv[2])

yearrange=1-startyear+endyear
num_full_nodes=yearrange//3

comm=MPI.COMM_WORLD
size=comm.Get_size()
rank=comm.Get_rank()
mynh = np.zeros(365)
mysh = np.zeros(365)


if (rank<num_full_nodes):
    numyears=0
    if (yearrange<3):
        numyears=yearrange
    else:
        numyears=3
    mystartyear=startyear+(3*(rank))

    processAction(rank,mystartyear,numyears)

    
elif (rank==num_full_nodes):
    numyears=yearrange%3
    mystartyear=startyear+(yearrange//3)*3

    processAction(rank,mystartyear,numyears)

if (rank==0):
    t1 = MPI.Wtime()
    mmpday_nh=mmpday_nh/yearrange
    mmpday_sh=mmpday_sh/yearrange
    sh_stats=Stats(np.sort(mmpday_sh))
    sh_stats.getStats()
    nh_stats=Stats(np.sort(mmpday_nh))
    nh_stats.getStats()
    t2 = MPI.Wtime() 
    totalSerialt = t2-t1
   
    print("NH stats are: \n")
    nh_stats.dispStats()
    print("SH stats are: \n")
    sh_stats.dispStats()
   
    print(f"Total Exec Time: {globalreducetime+timetaken+totalSerialt}")