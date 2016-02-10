# KVStore #

A distributed key-value store. CS240H Final Project.

[Vamsi Chitters](mailto:vamsikc@stanford.edu), [Colin Man](mailto:colinman.stanford.edu), [Nathan James Tindall](mailto:ntindall@stanford.edu) 

# Usage

To run the master node: `stack exec kvstore-master -- [options]`

To run a slave node: `stack exec kvstore-slave -- [options]`

To run a client node: `stack exec kvstore-client -- [options]`

```
KVStore
  -l      --local     local mode (configuration)
  -n Int  --size=Int  ring size  (number of slave nodes)
  -i Int  --id=Int    slaveId    (must be >= 0 and < ring size, only meaningful
                                  for slave nodes)
```