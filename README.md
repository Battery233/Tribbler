Tribbler
============

 A distributed twitter-like information dissemination service implemented in Golang.

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



## Instructions

### Compiling your code

To compile your code, execute one or more of the following commands (the resulting binaries will be located in the `$GOPATH/bin` directory):

```bash
go install github.com/cmu440/tribbler/runners/srunner
go install github.com/cmu440/tribbler/runners/lrunner
go install github.com/cmu440/tribbler/runners/trunner
go install github.com/cmu440/tribbler/runners/crunner
```

To simply check that your code compiles (i.e. without creating the binaries),
you can use the `go build` subcommand to compile an individual package as shown below:

```bash
# Build/compile the "tribserver" package.
go build path/to/tribserver
```



### Running your code

To run and test the individual components that make up the Tribbler system, we have provided
four simple programs that aim to simplify the process. The programs are located in the
`p3/src/github.com/cmu440/tribbler/runners/` directory and may be executed from anywhere on your system (after `go install`-ing them as discussed above).
Each program is discussed individually below:

##### The `srunner` program

The `srunner` (`StorageServer`-runner) program creates and runs an instance of your
`StorageServer` implementation. Some example usage is provided below (this assumes you are in the `$GOPATH/bin` directory):

```bash
# Start a single master storage server on port 9009.
./srunner -port=9009

# Start the master on port 9009 and run two additional slaves.
./srunner -port=9009 -N=3
./srunner -master="localhost:9009"
./srunner -master="localhost:9009"
```

Note that in the above example you do not need to specify a port for your slave storage servers.
For additional usage instructions, please execute `./srunner -help` or consult the `srunner.go` source code.

##### The `lrunner` program

The `lrunner` (`Libstore`-runner) program creates and runs an instance of your `Libstore`
implementation. It enables you to execute `Libstore` methods from the command line, as shown
in the example below:

```bash
# Create one (or more) storage servers in the background.
./srunner -port=9009 &

# Execute Put("thom", "yorke")
./lrunner -port=9009 p thom yorke
OK

# Execute Get("thom")
./lrunner -port=9009 g thom
yorke

# Execute Get("jonny")
./lrunner -port=9009 g jonny
ERROR: Get operation failed with status KeyNotFound
```

Note that the exact error messages that are output by the `lrunner` program may differ
depending on how you implement your `Libstore`. For additional usage instructions, please
execute `./lrunner -help` or consult the `lrunner.go` source code.

##### The `trunner` program

The `trunner` (`TribServer`-runner) program creates and runs an instance of your
`TribServer` implementation. For usage instructions, please execute `./trunner -help` or consult the
`trunner.go` source code. In order to use this program for your own personal testing,
your `Libstore` implementation must function properly and one or more storage servers
(i.e. `srunner` programs) must be running in the background.

##### The `crunner` program

The `crunner` (`TribClient`-runner) program creates and runs an instance of the
`TribClient` implementation we have provided as part of the starter code.
For usage instructions, please execute `./crunner -help` or consult the
`crunner.go` source code. As with the above programs, you'll need to start one or
more Tribbler servers and storage servers beforehand so that the `TribClient`
will have someone to communicate with.



### Tests

The tests are provided as bash shell scripts in the `p3/tests_cp` and `p3/tests` directory.
The scripts may be run from anywhere on your system (assuming your `GOPATH` has been set and
they are being executed on a 64-bit Mac OS X or Linux machine). For example, to run the
`libtest.sh` test, simply execute the following:

```bash
$GOPATH/tests/libtest.sh
```
