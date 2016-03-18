# Aergia #

A distributed key-value store. Implemented in Haskell. CS240H Final Project.

[Vamsi Chitters](mailto:vamsikc@stanford.edu),
[Colin Man](mailto:colinman.stanford.edu),
[Nathan James Tindall](mailto:ntindall@stanford.edu)

[PDF writeup](https://github.com/ntindall/KVStore/blob/master/pdf/aergia-cs240h-chitters-man-tindall.pdf)

# Usage

To run the master node: `stack exec kvstore-master -- [options]`

To run a worker node: `stack exec kvstore-worker -- [options]`

To run a client node: `stack exec kvstore-client -- [options]`

```
KVStore
  -l      --local     local mode (configuration)
  -n Int  --size=Int  ring size  (number of worker nodes)
  -i Int  --id=Int    workerId    (must be >= 0 and < ring size, only meaningful
                                  for worker nodes)
```
