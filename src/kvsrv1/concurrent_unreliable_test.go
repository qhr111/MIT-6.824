package kvsrv

import (
	"testing"
	"time"

	kvtest "6.5840/kvtest1"
)

// Test concurrent clients in unreliable network - this is the missing test case
// that could expose linearizability issues under race conditions
func TestPutConcurrentUnreliable(t *testing.T) {
	const (
		PORCUPINETIME = 30 * time.Second // Longer timeout for unreliable network
		NCLNT         = 5                // Multiple clients
		NSEC          = 2                // Run for 2 seconds
	)

	ts := MakeTestKV(t, false) // false = unreliable network
	defer ts.Cleanup()

	ts.Begin("Test: many clients racing to put values in unreliable network")

	rs := ts.SpawnClientsAndWait(NCLNT, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		return ts.OneClientPut(me, ck, []string{"k"}, done)
	})
	
	ck := ts.MakeClerk()
	ts.CheckPutConcurrent(ck, "k", rs, &kvtest.ClntRes{}, ts.IsReliable())
	
	// This is the critical test - check if all operations are linearizable
	// even under concurrent access with unreliable network
	ts.CheckPorcupineT(PORCUPINETIME)
}

// Test multiple keys with concurrent clients in unreliable network
func TestMultiKeyConcurrentUnreliable(t *testing.T) {
	const (
		PORCUPINETIME = 30 * time.Second
		NCLNT         = 3
		NSEC          = 2
	)

	ts := MakeTestKV(t, false) // unreliable network
	defer ts.Cleanup()

	ts.Begin("Test: concurrent clients on multiple keys in unreliable network")

	keys := []string{"key1", "key2", "key3"}
	
	_ = ts.SpawnClientsAndWait(NCLNT, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		return ts.OneClientPut(me, ck, keys, done)
	})
	
	// Just check overall linearizability - don't check individual keys
	// since some keys might not exist in unreliable network scenarios
	ts.CheckPorcupineT(PORCUPINETIME)
}