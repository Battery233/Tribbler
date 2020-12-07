package storageserver

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/util"
)

type storageServer struct {
	isAlive bool       // DO NOT MODIFY
	mux     sync.Mutex // DO NOT MODIFY

	numNodes       int               // number of nodes in the system
	hostPort       string            // the hostPort
	port           int               // port number
	virtualIDs     []uint32          // unique identifier of the server
	servers        []storagerpc.Node // all servers within the system
	valueMap       map[string]string // map for storing values of Get
	valueMapMux    sync.Mutex
	listMap        map[string][]string // map for storing values of GetList
	listMapMux     sync.Mutex
	cacheRecords   map[string]leaseRemains // cache record for a specific key string
	cacheRecordMux sync.Mutex
}

type leaseRemains map[string]int //map of port to seconds remains

//todo remove prints

// USED FOR TESTS, DO NOT MODIFY
func (ss *storageServer) SetAlive(alive bool) {
	ss.mux.Lock()
	ss.isAlive = alive
	ss.mux.Unlock()
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's hostPort:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// virtualIDs is a list of random, unsigned 32-bits IDs identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, virtualIDs []uint32) (StorageServer, error) {
	/****************************** DO NOT MODIFY! ******************************/
	ss := new(storageServer)
	ss.isAlive = true
	/****************************************************************************/

	hostPort := "localhost:" + strconv.Itoa(port)
	listener, err := net.Listen("tcp", hostPort)
	if err != nil {
		return nil, err
	}

	ss.port = port
	ss.hostPort = hostPort
	ss.virtualIDs = virtualIDs
	ss.numNodes = numNodes
	ss.servers = make([]storagerpc.Node, 0)
	ss.valueMap = make(map[string]string)
	ss.listMap = make(map[string][]string)
	ss.cacheRecords = make(map[string]leaseRemains)

	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}

	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	if masterServerHostPort == "" { // is master
		masterNode := storagerpc.Node{HostPort: hostPort, VirtualIDs: virtualIDs}
		ss.servers = append(ss.servers, masterNode)
		for {
			ss.mux.Lock()
			ready := len(ss.servers) == ss.numNodes
			ss.mux.Unlock()
			if ready {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	} else { // this server is a slave
		masterServer, _ := rpc.DialHTTP("tcp", masterServerHostPort)
		serverInfo := storagerpc.Node{HostPort: hostPort, VirtualIDs: virtualIDs}
		args := storagerpc.RegisterArgs{ServerInfo: serverInfo}
		reply := storagerpc.RegisterReply{}
		for {
			err := masterServer.Call("StorageServer.RegisterServer", &args, &reply)
			if err != nil {
				fmt.Printf("slave call master errer: %v", err)
				return nil, err
			}
			if reply.Status != storagerpc.OK {
				time.Sleep(time.Second)
			} else {
				ss.servers = reply.Servers
				break
			}
		}
	}

	go ss.cacheManager()

	return ss, nil
}

func (ss *storageServer) registerServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.mux.Lock()
	defer ss.mux.Unlock()

	slave := args.ServerInfo
	alreadyRegistered := false
	for _, registeredSlave := range ss.servers {
		if util.CompareUint32Slice(registeredSlave.VirtualIDs, slave.VirtualIDs) {
			alreadyRegistered = true
			break
		}
	}
	if !alreadyRegistered {
		ss.servers = append(ss.servers, slave)
	}

	reply.Servers = ss.servers
	if len(ss.servers) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) getServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.mux.Lock()
	defer ss.mux.Unlock()
	reply.Servers = ss.servers
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	ss.valueMapMux.Lock()
	defer ss.valueMapMux.Unlock()
	value, ok := ss.valueMap[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
	if args.WantLease {
		reply.Lease = storagerpc.Lease{
			Granted:      true,
			ValidSeconds: storagerpc.LeaseSeconds,
		}
		ss.cacheRecordMux.Lock()
		if ss.cacheRecords[args.Key] == nil {
			ss.cacheRecords[args.Key] = make(leaseRemains)
		}
		ss.cacheRecords[args.Key][args.HostPort] = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
		ss.cacheRecordMux.Unlock()
	}
	reply.Status = storagerpc.OK
	reply.Value = value
	return nil
}

func (ss *storageServer) delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	ss.valueMapMux.Lock()
	defer ss.valueMapMux.Unlock()
	_, ok := ss.valueMap[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
	delete(ss.valueMap, args.Key)
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) getList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	ss.listMapMux.Lock()
	defer ss.listMapMux.Unlock()
	value, ok := ss.listMap[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
	if args.WantLease {
		reply.Lease = storagerpc.Lease{
			Granted:      true,
			ValidSeconds: storagerpc.LeaseSeconds,
		}
		ss.cacheRecordMux.Lock()
		if ss.cacheRecords[args.Key] == nil {
			ss.cacheRecords[args.Key] = make(leaseRemains)
		}
		ss.cacheRecords[args.Key][args.HostPort] = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
		ss.cacheRecordMux.Unlock()
	}
	reply.Status = storagerpc.OK
	reply.Value = value
	return nil
}

func (ss *storageServer) put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.valueMapMux.Lock()
	defer ss.valueMapMux.Unlock()
	ss.removeLeases(args.Key)
	ss.valueMap[args.Key] = args.Value
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) appendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.listMapMux.Lock()
	defer ss.listMapMux.Unlock()
	_, ok := ss.listMap[args.Key]
	if !ok {
		ss.listMap[args.Key] = make([]string, 0)
	} else {
		ls := ss.listMap[args.Key]
		for _, val := range ls {
			if val == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
	}
	ss.removeLeases(args.Key)
	ss.listMap[args.Key] = append(ss.listMap[args.Key], args.Value)
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) removeFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.listMapMux.Lock()
	defer ss.listMapMux.Unlock()
	ls, ok := ss.listMap[args.Key]
	if ok {
		index := -1
		for i, val := range ls {
			if val == args.Value {
				index = i
				break
			}
		}
		if index != -1 {
			ss.removeLeases(args.Key)
			ss.listMap[args.Key] = append(ls[:index], ls[index+1:]...)
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
	} else {
		//todo test it type
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) cacheManager() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			ss.cacheRecordMux.Lock()
			for key, leases := range ss.cacheRecords {
				for port := range leases {
					leases[port]--
					if leases[port] == 0 {
						delete(leases, port)
					}
				}
				if len(leases) == 0 {
					delete(ss.cacheRecords, key)
				}
			}
			ss.cacheRecordMux.Unlock()
		}
	}
}

func (ss *storageServer) removeLeases(key string) {
	ss.cacheRecordMux.Lock()
	defer ss.cacheRecordMux.Unlock()
	if leases, ok := ss.cacheRecords[key]; ok {
		revokeResponseChannel := make(chan bool)
		for port := range leases {
			go func(port string, revokeResponseChannel chan bool) {
				leaseHolder, _ := rpc.DialHTTP("tcp", port)
				args := &storagerpc.RevokeLeaseArgs{Key: key}
				reply := &storagerpc.RevokeLeaseReply{}
				// TODO: if fail, re-call RevokeLease?
				if err := leaseHolder.Call("LeaseCallbacks.RevokeLease", args, reply); err != nil {
					fmt.Println("Revoke Lease return err not nil!")
					revokeResponseChannel <- false
				} else if reply.Status == storagerpc.OK || reply.Status == storagerpc.KeyNotFound {
					revokeResponseChannel <- true
				}
			}(port, revokeResponseChannel)
		}
		for i := 0; i < len(leases); i++ {
			<-revokeResponseChannel
		}
	}
}