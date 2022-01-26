# Global Rainfall Analysis

This project aims to draw a box and whisker plot from a NETCDF dataset which contains precipitation data per day organised by year. Message Passing Interface is used as a method for Single Program Multiple Data parallelisation. The program is written in python and implemented on a cluster of nodes using the SLURM workload manager. 

## Running the code
sbatch proj.slurm

## Requirements
Specify the path to the source code in the slurm file\
The number of tasks and nodes are required to be N/3 where N are the number of years and N/3 is an integer division.

## Output
The output is sent to a file 'slurm-{job-number}.out' by the workload manager
