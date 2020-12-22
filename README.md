Tribbler
============

 A distributed twitter-like information dissemination service implemented in GoLang.

The code for this project is organized roughly as follows:

```
bin/                               
sols/                              Compiled binaries

src/github.com/cmu440/tribbler/
  tribclient/                      TribClient implementation
  tribserver/                      TribServer implementation
  libstore/                        Libstore implementation
  storageserver/                   StorageServer implementation

  util/                            Util functions
    keyFormatter.go                Format the key posted to storage server

  tests/                               
  tests_cp/                        Tests source code

  rpc/
    tribrpc/                       TribServer RPC helpers/constants
    librpc/                        Libstore RPC helpers/constants
    storagerpc/                    StorageServer RPC helpers/constants

tests_cp/                          Shell scripts to run the test for a checkpoint implementation
tests/                             Shell scripts to run the tests for a final implementation
```
