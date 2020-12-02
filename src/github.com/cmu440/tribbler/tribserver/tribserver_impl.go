package tribserver

import (
	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"

	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	// TODO: implement this!
	lStore   libstore.Libstore
	listener net.Listener
}

/*
k - v
abc:userid  -   ""  -> user register
abc:sublist -   [bcd, cde] -> abc subscribes two users
abc:triblist -  [id1, id2] ->post id by abc
id1  -    jsonPostContent  -> content of a post (id1)
*/

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	lStore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	tServer := &tribServer{
		lStore:   lStore,
		listener: listener,
	}
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tServer))
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	return tServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	if ts.testIfUserExist(args.UserID) {
		reply.Status = tribrpc.Exists
		return nil
	}
	err := ts.lStore.Put(util.FormatUserKey(args.UserID), "")
	if err != nil {
		reply.Status = tribrpc.Exists
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	//todo potential optimization
	if !ts.testIfUserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if !ts.testIfUserExist(args.TargetUserID) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	if err := ts.lStore.AppendToList(util.FormatSubListKey(args.UserID), args.TargetUserID); err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	//todo potential optimization
	if !ts.testIfUserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if !ts.testIfUserExist(args.TargetUserID) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	if err := ts.lStore.RemoveFromList(util.FormatSubListKey(args.UserID), args.TargetUserID); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	reply.UserIDs = make([]string, 0)
	if !ts.testIfUserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if list, err := ts.lStore.GetList(util.FormatSubListKey(args.UserID)); err == nil { //err == nil means the user has subscriptions
		for _, s := range list {
			if !ts.testIfUserExist(s) {
				reply.Status = tribrpc.NoSuchUser
				return nil
			}
			if subscriberList, err := ts.lStore.GetList(util.FormatSubListKey(s)); err == nil { // has subscribed users
				for _, id := range subscriberList {
					if id == args.UserID {
						reply.UserIDs = append(reply.UserIDs, s)
						break
					}
				}
			}
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	if !ts.testIfUserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	var tribble tribrpc.Tribble
	var tribbleId string
	for {
		now := time.Now()
		tribble = tribrpc.Tribble{UserID: args.UserID, Posted: now, Contents: args.Contents}
		tribbleId = util.FormatPostKey(args.UserID, now.UnixNano())
		if _, err := ts.lStore.Get(tribbleId); err != nil {
			break
		}
	}
	marshaledTribble, _ := json.Marshal(tribble)
	reply.PostKey = tribbleId
	if err := ts.lStore.AppendToList(util.FormatTribListKey(args.UserID), tribbleId); err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	if err := ts.lStore.Put(tribbleId, string(marshaledTribble)); err != nil {
		//todo to recover from previous err and make sure it is consistent
		ts.lStore.RemoveFromList(util.FormatTribListKey(args.UserID), tribbleId)
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	if !ts.testIfUserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	err := ts.lStore.RemoveFromList(util.FormatTribListKey(args.UserID), args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	err = ts.lStore.Delete(args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if !ts.testIfUserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	reply.Tribbles = make([]tribrpc.Tribble, 0)
	if list, err := ts.lStore.GetList(util.FormatTribListKey(args.UserID)); err == nil { //if the user posted before
		//todo what if his post is in three servers && optimize?
		for _, id := range list {
			item, err := ts.lStore.Get(id)
			if err != nil {
				reply.Status = tribrpc.NoSuchPost
				return nil
			}
			var t tribrpc.Tribble
			err = json.Unmarshal([]byte(item), &t)
			reply.Tribbles = append(reply.Tribbles, t)
		}
		sort.Sort(ReverseTimeSorter(reply.Tribbles))
		if len(reply.Tribbles) > 100 {
			reply.Tribbles = reply.Tribbles[:100]
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if !ts.testIfUserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	reply.Tribbles = make([]tribrpc.Tribble, 0)
	subscriptions, err := ts.lStore.GetList(util.FormatSubListKey(args.UserID))
	if err != nil { //the user has no subscription
		reply.Status = tribrpc.OK
		return nil
	}

	for _, sub := range subscriptions {
		tribbleIds, err := ts.lStore.GetList(util.FormatTribListKey(sub))
		if err != nil { //the subscribed author has no tribbles
			continue
		}
		for _, tribbleId := range tribbleIds {
			tribble, err := ts.lStore.Get(tribbleId)
			if err != nil {
				reply.Status = tribrpc.NoSuchPost
				return nil
			}
			var t tribrpc.Tribble
			json.Unmarshal([]byte(tribble), &t)
			reply.Tribbles = append(reply.Tribbles, t)
		}
	}
	sort.Sort(ReverseTimeSorter(reply.Tribbles))
	if len(reply.Tribbles) > 100 {
		reply.Tribbles = reply.Tribbles[:100]
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) testIfUserExist(userId string) bool {
	emptyString, err := ts.lStore.Get(util.FormatUserKey(userId))
	return err == nil && emptyString == ""
}

type ReverseTimeSorter []tribrpc.Tribble

func (ts ReverseTimeSorter) Len() int           { return len(ts) }
func (ts ReverseTimeSorter) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts ReverseTimeSorter) Less(i, j int) bool { return ts[i].Posted.After(ts[j].Posted) }
