DistributedCB
=============

A Parallel and Distributed Content-Based Recommender System

This is the Python implementation of the parallel and distributed content-based recommender system that was accepted for publication in the Journal of Intelligent Information Systems (JIIS) in 2013. The code provided here may serve as a reference for others interested in parallel and distributed recommender systems. Please cite the corresponding paper if you make use of this work.

    @article{dooms2013distributedcb,
      title={In-Memory, Distributed Content-Based Recommender System},
      author={Dooms, Simon and Audenaert, Pieter and Fostier, Jan and De Pessemier, Toon and Martens, Luc},
      journal={Journal of Intelligent Information Systems},
      doi={10.1007/s10844-013-0276-1},
      year={2013},
      publisher={Springer}
    }

First, the work_division.py file should be used to pre-process the input data so that jobs can be distributed in a load balanced way (see the paper for more details). Next, the distributed_CB_recommender.py Python file can be used to calculate the actual recommendations. By changing the input parameters, the number of computing nodes (and cores per node) can be set. 

This work was carried out using the Stevin Supercomputer Infrastructure at Ghent University, funded by Ghent University, the Hercules Foundation and the Flemish Government – department EWI. The scripts used to submit jobs on this HPC infrastructure are not provided here, as they are too system-specific. Contact Simon Dooms if these can be of any use for you.

The DistributedCB recommender system does not rely on Mahout or other MapReduce related frameworks, instead recommendation logic is parallelized using the Python 'multiprocessing' module. Recommendation work is distributed across different computing nodes and every node processes its work in-memory, meaning that data will be kept in RAM from start to end for efficiency reasons. Our approach does not impose any file system requirements and can be run on any machine capable of running Python code.

This code is intended mainly for academic use, and will therefore not be maintained.