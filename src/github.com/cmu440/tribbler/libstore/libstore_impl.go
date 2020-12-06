package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"math"
	"net/rpc"
	"strings"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	storageServers      map[uint32]*rpc.Client
	myHostPort          string
	leaseMode           LeaseMode
}

const MaximumAttempt = 5

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	masterStorage, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	args := &storagerpc.GetServersArgs{}
	reply := &storagerpc.GetServersReply{}
	for attempted := 0; attempted < MaximumAttempt; attempted++ {
		err := masterStorage.Call("StorageServer.GetServers", args, reply)
		if err != nil || reply.Status != storagerpc.OK {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	if reply.Status != storagerpc.OK {
		return nil, errors.New("connect to storage failed after 5 attempts")
	}

	dialClients := make(map[uint32]*rpc.Client)
	for _, node := range reply.Servers {
		ss, err := rpc.DialHTTP("tcp", node.HostPort)
		if err != nil {
			return nil, err
		}
		for _,id:=range node.VirtualIDs{
			dialClients[id] = ss
		}
	}

	lStore := &libstore{
		storageServers:      dialClients,
		myHostPort:          myHostPort,
		leaseMode:           mode,
	}
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(lStore))
	if err != nil {
		return nil, err
	}
	return lStore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// TODO: get routed server
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: false,         //todo lease here
		HostPort:  ls.myHostPort, //todo
	}
	reply := &storagerpc.GetReply{}
	if err := ls.chooseStorageServer(key).Call("StorageServer.Get", args, reply); err != nil {
		return "", err
	}
	if reply.Status != storagerpc.OK {
		return "", errors.New("get key not found")
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	reply := &storagerpc.PutReply{}
	if err := ls.chooseStorageServer(key).Call("StorageServer.Put", args, reply); err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New("put error")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	reply := &storagerpc.DeleteReply{}
	if err := ls.chooseStorageServer(key).Call("StorageServer.Delete", args, reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("delete error: return value not OK")
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: false,         //todo lease here
		HostPort:  ls.myHostPort, //todo
	}
	reply := &storagerpc.GetListReply{}
	if err := ls.chooseStorageServer(key).Call("StorageServer.GetList", args, reply); err != nil {
		return nil, err
	}
	if reply.Status != storagerpc.OK {
		return nil, errors.New("getList key not found")
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	reply := &storagerpc.PutReply{}
	if err := ls.chooseStorageServer(key).Call("StorageServer.RemoveFromList", args, reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.KeyNotFound {
		return errors.New("remove from list key not found")
	} else if reply.Status == storagerpc.ItemNotFound {
		return errors.New("remove from list item not found")
	} else if reply.Status != storagerpc.OK {
		return errors.New("remove from list unexpected non-ok status")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	reply := &storagerpc.PutReply{}
	if err := ls.chooseStorageServer(key).Call("StorageServer.AppendToList", args, reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.ItemExists {
		return errors.New("error status ItemExists")
	} else if reply.Status != storagerpc.OK {
		return errors.New("unexpected error in AppendToList")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	//todo implement it
	return nil
}

func (ls *libstore) chooseStorageServer(key string) *rpc.Client {
	key = strings.Split(key,":")[0]
	hash := StoreHash(key)

	var resultKey uint32 = math.MaxUint32
	var minKey uint32 = math.MaxUint32

	for k,v:=range ls.storageServers{
		if k < minKey {
			minKey = k
		}

		if k==hash {
			return v
		} else if k > hash && k < resultKey {
			resultKey = k
		}
	}
	if _, ok := ls.storageServers[math.MaxUint32]; resultKey == math.MaxUint32 && !ok {
		return ls.storageServers[minKey]
	}
	return ls.storageServers[resultKey]
}
