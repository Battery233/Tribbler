package libstore

import (
	"errors"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/util"
)

type libstore struct {
	storageServers  map[uint32]*rpc.Client // map that stores all rpc connections
	sortedServerIDs []uint32               // this is needed to perform binary search
	myHostPort      string
	leaseMode       LeaseMode
	localCache      localCache
	queryCounter    queryCounter // a 2D array for identifying which item needs to be cached
}

type localCache struct {
	mux   sync.Mutex
	cache map[string]cachedValue
}

// cachedValue can either store a single value cache or a list cache, and it
// keeps track of the remaining lease time in seconds
type cachedValue struct {
	value          string
	list           []string
	remainingLease int
}

type queryCounter struct {
	mux        sync.Mutex
	keyHistory [][]string
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
	sortedIDs := make([]uint32, 0)
	for _, node := range reply.Servers {
		ss, err := rpc.DialHTTP("tcp", node.HostPort)
		if err != nil {
			return nil, err
		}
		for _, id := range node.VirtualIDs {
			dialClients[id] = ss
			sortedIDs = append(sortedIDs, id)
		}
	}

	// sort it so that we can perform binary search
	sort.Sort(util.UInt32Sorter(sortedIDs))
	lStore := &libstore{
		storageServers:  dialClients,
		myHostPort:      myHostPort,
		leaseMode:       mode,
		sortedServerIDs: sortedIDs,
		queryCounter: queryCounter{
			mux:        sync.Mutex{},
			keyHistory: make([][]string, 0),
		},
		localCache: localCache{
			mux:   sync.Mutex{},
			cache: make(map[string]cachedValue),
		},
	}
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(lStore))
	if err != nil {
		return nil, err
	}
	lStore.queryCounter.keyHistory = append(lStore.queryCounter.keyHistory, make([]string, 0)) //add the history slot for the first second

	go lStore.cacheManager()

	return lStore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// keep track of what key is being get so that we know which one to keep cache of
	ls.queryCounter.mux.Lock()
	lastIndex := len(ls.queryCounter.keyHistory) - 1
	ls.queryCounter.keyHistory[lastIndex] = append(ls.queryCounter.keyHistory[lastIndex], key)
	ls.queryCounter.mux.Unlock()

	ls.localCache.mux.Lock()
	cache, ok := ls.localCache.cache[key]
	ls.localCache.mux.Unlock()
	if ok {
		return cache.value, nil
	}

	wantLease := ls.checkWantLease(key)
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.myHostPort,
	}
	reply := &storagerpc.GetReply{}
	storageServerIndex := ls.chooseStorageServer(key)
	for i := 0; i < len(ls.sortedServerIDs); i++ {
		err := ls.storageServers[ls.sortedServerIDs[storageServerIndex]].Call("StorageServer.Get", args, reply)
		if err == nil {
			break
		} else {
			storageServerIndex = (storageServerIndex + 1) % len(ls.storageServers)
		}
	}
	if reply.Status != storagerpc.OK {
		return "", errors.New("get key not found")
	}

	if wantLease && reply.Lease.Granted {
		ls.localCache.mux.Lock()
		ls.localCache.cache[key] = cachedValue{
			value:          reply.Value,
			list:           nil,
			remainingLease: reply.Lease.ValidSeconds,
		}
		ls.localCache.mux.Unlock()
	}

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	reply := &storagerpc.PutReply{}
	storageServerIndex := ls.chooseStorageServer(key)
	for i := 0; i < len(ls.sortedServerIDs); i++ {
		err := ls.storageServers[ls.sortedServerIDs[storageServerIndex]].Call("StorageServer.Put", args, reply)
		if err == nil {
			break
		} else {
			storageServerIndex = (storageServerIndex + 1) % len(ls.storageServers)
		}
	}
	if reply.Status != storagerpc.OK {
		return errors.New("put error")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	reply := &storagerpc.DeleteReply{}
	ls.localCache.mux.Lock()
	if _, ok := ls.localCache.cache[key]; ok {
		delete(ls.localCache.cache, key)
	}
	ls.localCache.mux.Unlock()

	storageServerIndex := ls.chooseStorageServer(key)
	for i := 0; i < len(ls.sortedServerIDs); i++ {
		err := ls.storageServers[ls.sortedServerIDs[storageServerIndex]].Call("StorageServer.Delete", args, reply)
		if err == nil {
			break
		} else {
			storageServerIndex = (storageServerIndex + 1) % len(ls.storageServers)
		}
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("delete error: return value not OK")
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.queryCounter.mux.Lock()
	lastIndex := len(ls.queryCounter.keyHistory) - 1
	ls.queryCounter.keyHistory[lastIndex] = append(ls.queryCounter.keyHistory[lastIndex], key)
	ls.queryCounter.mux.Unlock()

	ls.localCache.mux.Lock()
	cache, ok := ls.localCache.cache[key]
	ls.localCache.mux.Unlock()
	if ok {
		return cache.list, nil
	}

	wantLease := ls.checkWantLease(key)
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.myHostPort,
	}
	reply := &storagerpc.GetListReply{}
	storageServerIndex := ls.chooseStorageServer(key)
	for i := 0; i < len(ls.sortedServerIDs); i++ {
		err := ls.storageServers[ls.sortedServerIDs[storageServerIndex]].Call("StorageServer.GetList", args, reply)
		if err == nil {
			break
		} else {
			storageServerIndex = (storageServerIndex + 1) % len(ls.storageServers)
		}
	}

	if reply.Status != storagerpc.OK {
		return nil, errors.New("getList key not found")
	}

	if wantLease && reply.Lease.Granted {
		ls.localCache.mux.Lock()
		ls.localCache.cache[key] = cachedValue{
			value:          "",
			list:           reply.Value,
			remainingLease: reply.Lease.ValidSeconds,
		}
		ls.localCache.mux.Unlock()
	}

	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	reply := &storagerpc.PutReply{}
	ls.localCache.mux.Lock()
	if _, ok := ls.localCache.cache[key]; ok {
		delete(ls.localCache.cache, key)
	}
	ls.localCache.mux.Unlock()
	storageServerIndex := ls.chooseStorageServer(key)
	for i := 0; i < len(ls.sortedServerIDs); i++ {
		err := ls.storageServers[ls.sortedServerIDs[storageServerIndex]].Call("StorageServer.RemoveFromList", args, reply)
		if err == nil {
			break
		} else {
			storageServerIndex = (storageServerIndex + 1) % len(ls.storageServers)
		}
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
	storageServerIndex := ls.chooseStorageServer(key)
	for i := 0; i < len(ls.sortedServerIDs); i++ {
		err := ls.storageServers[ls.sortedServerIDs[storageServerIndex]].Call("StorageServer.AppendToList", args, reply)
		if err == nil {
			break
		} else {
			storageServerIndex = (storageServerIndex + 1) % len(ls.storageServers)
		}
	}
	if reply.Status == storagerpc.ItemExists {
		return errors.New("error status ItemExists")
	} else if reply.Status != storagerpc.OK {
		return errors.New("unexpected error in AppendToList")
	}
	return nil
}

// RevokeLease is a RPC used by storageServer when Put was called
// delete local cache to mark that lease was revoked
func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.localCache.mux.Lock()
	defer ls.localCache.mux.Unlock()
	if _, ok := ls.localCache.cache[args.Key]; ok {
		delete(ls.localCache.cache, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

// chooseStorageServer has the main logic for consistent caching
func (ls *libstore) chooseStorageServer(key string) int {
	hash := StoreHash(strings.Split(key, ":")[0])
	servers := ls.sortedServerIDs

	if hash < servers[0] {
		return 0
	} else if hash > servers[len(servers)-1] {
		return len(servers) - 1
	}
	left, right := 0, len(servers)-1
	for left <= right {
		mid := left + (right-left)/2
		if servers[mid] == hash {
			return mid
		}
		if servers[mid] < hash {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	if left < len(servers) {
		return right
	}
	return left
}

// cacheManager is a go routine that decrements the remaining lease that we
// currently hold by one for each second. When any one reaches 0, we want to
// revoke the lease and delete the cache.
func (ls *libstore) cacheManager() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			ls.queryCounter.mux.Lock()
			if len(ls.queryCounter.keyHistory) == storagerpc.QueryCacheSeconds {
				ls.queryCounter.keyHistory = ls.queryCounter.keyHistory[1:]
			}
			ls.queryCounter.keyHistory = append(ls.queryCounter.keyHistory, make([]string, 0))
			ls.queryCounter.mux.Unlock()

			ls.localCache.mux.Lock()
			for k, v := range ls.localCache.cache {
				v.remainingLease--
				if v.remainingLease == 0 {
					delete(ls.localCache.cache, k)
				}
			}
			ls.localCache.mux.Unlock()
		}
	}
}

// when reached query cache threshold, we want to cache the value locally (but it also
// depends on the lease mode)
func (ls *libstore) reachQueryCacheThresh(key string) bool {
	ls.queryCounter.mux.Lock()
	defer ls.queryCounter.mux.Unlock()
	counter := 0
	for _, records := range ls.queryCounter.keyHistory {
		for _, s := range records {
			if s == key {
				counter++
			}
			if counter == storagerpc.QueryCacheThresh {
				return true
			}
		}
	}
	return false
}

// checkWantLease returns whether to request lease according to the current settings
func (ls *libstore) checkWantLease(key string) bool {
	if ls.leaseMode == Never || ls.myHostPort == "" {
		return false
	}
	if ls.leaseMode == Always {
		return true
	}
	return ls.reachQueryCacheThresh(key)
}
