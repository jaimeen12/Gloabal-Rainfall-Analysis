#!/bin/bash
#SBATCH -p jaguarcluster
#SBATCH -J MPItest          # Name the job as MPItest
#SBATCH --cpus-per-task=1
#SBATCH --nodes=1
#SBATCH --ntasks=1 # Request n cores or task per node
#SBATCH -e MPItest-%j.err
#SBATCH -t 0-00:03:00 
#SBATCH --mem-per-cpu=1000   # Request 4000MB (4GB) RAM per core



#module load OpenMPI                 # will list modules loaded by default. In our case, it will be GNU8 compilers and OpenMPI3 MPI librarie 
pwd                         # prints current working directory
date                        # prints the date and time

srun --mpi=pmi2  python3 /home/group13/practice/rain.py 1982 1984 2018 