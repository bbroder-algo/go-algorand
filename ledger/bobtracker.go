// Copyright (C) 2019-2023 Algorand, Inc.
// This file is part of go-algorand
//
// go-algorand is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// go-algorand is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with go-algorand.  If not, see <https://www.gnu.org/licenses/>.

package ledger

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"time"

	"github.com/algorand/go-deadlock"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/crypto/bobtrie"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/ledger/store/trackerdb"
	"github.com/algorand/go-algorand/logging"
)

const (
	// trieRebuildAccountChunkSize defines the number of accounts that would get read at a single chunk
	// before added to the trie during trie construction
	bobtrieRebuildAccountChunkSize = 16384
	// trieRebuildCommitFrequency defines the number of accounts that would get added before we call evict to commit the changes and adjust the memory cache.
	bobtrieRebuildCommitFrequency = 65536
)

type bobTracker struct {
	// dbDirectory is the directory where the ledger and block sql file resides as well as the parent directory for the catchup files to be generated
	dbDirectory string

	// catchpointInterval is the configured interval at which the bobTracker would generate catchpoint labels and catchpoint files.
	catchpointInterval uint64

	// catchpointFileHistoryLength defines how many catchpoint files we want to store back.
	// 0 means don't store any, -1 mean unlimited and positive number suggest the number of most recent catchpoint files.
	catchpointFileHistoryLength int

	// enableGeneratingCatchpointFiles determines whether catchpoints files should be generated by the trackers.
	enableGeneratingCatchpointFiles bool

	// Prepared SQL statements for fast accounts DB lookups.
	accountsq trackerdb.AccountsReader

	// log copied from ledger
	log logging.Logger

	// Connection to the database.
	dbs             trackerdb.Store
//	catchpointStore trackerdb.CatchpointReaderWriter
	//	catchpointStore trackerdb.CatchpointReaderWriter

	// The last catchpoint label that was written to the database. Should always align with what's in the database.
	// note that this is the last catchpoint *label* and not the catchpoint file.
	lastCatchpointLabel string

	// catchpointDataSlowWriting suggests to the accounts writer that it should finish
	// writing up the (first stage) catchpoint data file ASAP. When this channel is
	// closed, the accounts writer would try and complete the writing as soon as possible.
	// Otherwise, it would take its time and perform periodic sleeps between chunks
	// processing.
	//	catchpointDataSlowWriting chan struct{}

	// catchpointDataWriting helps to synchronize the (first stage) catchpoint data file
	// writing. When this atomic variable is 0, no writing is going on.
	// Any non-zero value indicates a catchpoint being written, or scheduled to be written.
	//	catchpointDataWriting int32

	// The Trie tracking the current account balances. Always matches the balances that were
	// written to the database.
	balancesTrie *bobtrie.Trie

	// roundDigest stores the digest of the block for every round starting with dbRound+1 and every round after it.
	//	roundDigest []crypto.Digest

	// reenableCatchpointsRound is a round where the EnableCatchpointsWithSPContexts feature was enabled via the consensus.
	// we avoid generating catchpoints before that round in order to ensure the network remain consistent in the catchpoint
	// label being produced. This variable could be "wrong" in two cases -
	// 1. It's zero, meaning that the EnableCatchpointsWithSPContexts has yet to be seen.
	// 2. It's non-zero meaning that it the given round is after the EnableCatchpointsWithSPContexts was enabled ( it might be exact round
	//    but that's only if newBlock was called with that round ), plus the lookback.
	//	reenableCatchpointsRound basics.Round

	// forceCatchpointFileWriting used for debugging purpose by bypassing the test against
	// reenableCatchpointsRound in isCatchpointRound(), so that we could generate
	// catchpoint files even before the protocol upgrade took place.
	//	forceCatchpointFileWriting bool

	// catchpointsMu protects `roundDigest`, `reenableCatchpointsRound` and
	// `lastCatchpointLabel`.
	bobtrieMu deadlock.RWMutex
}

// initialize initializes the bobTracker structure
func (ct *bobTracker) initialize(cfg config.Local, dbPathPrefix string) {
	ct.dbDirectory = filepath.Dir(dbPathPrefix)

}

// handleUnorderedCommitOrError is a special method for handling deferred commits that are out of order.
// Tracker might update own state in this case. For example, account catchpoint tracker cancels
// scheduled catchpoint writing that deferred commit.
func (ct *bobTracker) handleUnorderedCommitOrError(dcc *deferredCommitContext) {
}

// loadFromDisk loads the state of a tracker from persistent
// storage.  The ledger argument allows loadFromDisk to load
// blocks from the database, or access its own state.  The
// ledgerForTracker interface abstracts away the details of
// ledger internals so that individual trackers can be tested
// in isolation.
func (ct *bobTracker) loadFromDisk(l ledgerForTracker, dbRound basics.Round) (err error) {
	ct.log = l.trackerLog()
	ct.log.Infof("bobtracker -- loadfromdisk")
	ct.dbs = l.trackerDB()
	//	ct.catchpointStore, err = l.trackerDB().MakeCatchpointReaderWriter()
	if err != nil {
		return err
	}

	//	ct.roundDigest = nil
	//	ct.catchpointDataWriting = 0
	// keep these channel closed if we're not generating catchpoint
	//	ct.catchpointDataSlowWriting = make(chan struct{}, 1)
	//	close(ct.catchpointDataSlowWriting)

	err = ct.dbs.Transaction(func(ctx context.Context, tx trackerdb.TransactionScope) error {
		return ct.initializeHashes(ctx, tx, dbRound)
	})
	if err != nil {
		return err
	}

	ct.accountsq, err = ct.dbs.MakeAccountsOptimizedReader()
	return
}

// newBlock informs the tracker of a new block from round
// rnd and a given ledgercore.StateDelta as produced by BlockEvaluator.
func (ct *bobTracker) newBlock(blk bookkeeping.Block, delta ledgercore.StateDelta) {
	//	ct.catchpointsMu.Lock()
	//	defer ct.catchpointsMu.Unlock()

	//	ct.roundDigest = append(ct.roundDigest, blk.Digest())
}

// committedUpTo implements the ledgerTracker interface for bobTracker.
// The method informs the tracker that committedRound and all it's previous rounds have
// been committed to the block database. The method returns what is the oldest round
// number that can be removed from the blocks database as well as the lookback that this
// tracker maintains.
func (ct *bobTracker) committedUpTo(rnd basics.Round) (retRound, lookback basics.Round) {
	return rnd, basics.Round(0)
}

// prepareCommit, commitRound and postCommit are called when it is time to commit tracker's data.
// If an error returned the process is aborted.
func (ct *bobTracker) prepareCommit(dcc *deferredCommitContext) error {
	//	ct.catchpointsMu.RLock()
	//	defer ct.catchpointsMu.RUnlock()
	//	ct.log.Infof("bobtracker -- preparecommit")

	//	dcc.committedRoundDigests = make([]crypto.Digest, dcc.offset)
	//	copy(dcc.committedRoundDigests, ct.roundDigest[:dcc.offset])

	return nil
}

func (ct *bobTracker) commitBobtrie(rnd basics.Round) (err error) {
	err = ct.dbs.Transaction(func(ctx context.Context, tx trackerdb.TransactionScope) error {
		aw, err := tx.MakeAccountsWriter()
		if err != nil {
			return err
		}
		var mc trackerdb.MerkleCommitter
		mc, err = tx.MakeBobCommitter(false)
		if err != nil {
			return err
		}
		var trie *bobtrie.Trie
		if ct.balancesTrie == nil {
			trie, err = bobtrie.MakeTrie(mc, trackerdb.BobTrieMemoryConfig)
			if err != nil {
				ct.log.Warnf("unable to create bob merkle trie during committedUpTo: %v", err)
				return err
			}
			ct.balancesTrie = trie
		} else {
			ct.balancesTrie.SetCommitter(mc)
		}
		_, err = ct.balancesTrie.Commit()
		root, rootErr := ct.balancesTrie.RootHash()
		if rootErr != nil {
			ct.log.Errorf("commitBobtrie: error retrieving balances trie root: %v", rootErr)
			return rootErr
		}
		ct.log.Infof("commitBobtrie: root: %v", root.String())
		aw.UpdateAccountsBobHashRound(ctx, rnd)
		if ct.balancesTrie != nil {
			_, err := ct.balancesTrie.Evict(false)
			if err != nil {
				ct.log.Warnf("bob merkle trie failed to evict: %v", err)
			}
		}
		return err
	})

	return err
}

func (ct *bobTracker) commitRound(ctx context.Context, tx trackerdb.TransactionScope, dcc *deferredCommitContext) (err error) {
	//	treeTargetRound := basics.Round(0)
	//	offset := dcc.offset
	//	dbRound := dcc.oldBase
	//	ct.log.Infof("bobtracker -- commitround %d", dbRound)
	//
	//	arw, err := tx.MakeAccountsReaderWriter()
	//	if err != nil {
	//		return err
	//	}
	//
	//	var mc trackerdb.MerkleCommitter
	//	mc, err = tx.MakeBobCommitter()
	//	if err != nil {
	//		return
	//	}
	//
	//	var trie *bobtrie.Trie
	//	if ct.balancesTrie == nil {
	//		trie, err = bobtrie.MakeTrie(mc, trackerdb.BobTrieMemoryConfig)
	//		if err != nil {
	//			ct.log.Warnf("unable to create bob merkle trie during committedUpTo: %v", err)
	//			return err
	//		}
	//		ct.balancesTrie = trie
	//	} else {
	//		ct.balancesTrie.SetCommitter(mc)
	//	}
	//	treeTargetRound = dbRound + basics.Round(offset)
	//
	//	dcc.stats.BobMerkleTrieUpdateDuration = time.Duration(time.Now().UnixNano())
	//
	//	err = ct.accountsUpdateBalances(dcc.compactAccountDeltas, dcc.compactResourcesDeltas, dcc.compactKvDeltas, dcc.oldBase, dcc.newBase())
	//	if err != nil {
	//		return err
	//	}
	//	now := time.Duration(time.Now().UnixNano())
	//	dcc.stats.BobMerkleTrieUpdateDuration = now - dcc.stats.BobMerkleTrieUpdateDuration
	//	ct.log.Infof("bobtracker -- commitround duration %d", dcc.stats.BobMerkleTrieUpdateDuration)
	//
	//	err = arw.UpdateAccountsBobHashRound(ctx, treeTargetRound)
	//	if err != nil {
	//		return err
	//	}
	//
	return nil
}

func (ct *bobTracker) postCommit(ctx context.Context, dcc *deferredCommitContext) {

	//	ct.catchpointsMu.Lock()
	//	ct.roundDigest = ct.roundDigest[dcc.offset:]
	//	ct.catchpointsMu.Unlock()

}

func (ct *bobTracker) postCommitUnlocked(ctx context.Context, dcc *deferredCommitContext) {
}

// handleUnorderedCommit is a special method for handling deferred commits that are out of order.
// Tracker might update own state in this case. For example, account catchpoint tracker cancels
// scheduled catchpoint writing that deferred commit.
func (ct *bobTracker) handleUnorderedCommit(dcc *deferredCommitContext) {
}

// close terminates the tracker, reclaiming any resources
// like open database connections or goroutines.  close may
// be called even if loadFromDisk() is not called or does
// not succeed.
func (ct *bobTracker) close() {
}

// accountsUpdateBalances applies the given compactAccountDeltas to the bobmerkle trie
func (ct *bobTracker) accountsUpdateBalances(accountsDeltas compactAccountDeltas, resourcesDeltas compactResourcesDeltas, kvDeltas map[string]modifiedKvValue, oldBase basics.Round, newBase basics.Round) (err error) {
	//	var added, deleted bool
	//	accumulatedChanges := 1
	ct.log.Infof("bobtracker -- accountsUpdatebalance")

	// We already did all this

	//	for i := 0; i < accountsDeltas.len(); i++ {
	//		delta := accountsDeltas.getByIdx(i)
	//		if !delta.oldAcct.AccountData.IsEmpty() {
	//			deleteHash := trackerdb.AccountHashBuilderV6(delta.address, &delta.oldAcct.AccountData, protocol.Encode(&delta.oldAcct.AccountData))
	//			deleted, err = ct.balancesTrie.Delete(deleteHash)
	//			if err != nil {
	//				return fmt.Errorf("failed to delete hash '%s' from bob merkle trie for account %v: %w", hex.EncodeToString(deleteHash), delta.address, err)
	//			}
	//			if !deleted {
	//				ct.log.Errorf("failed to delete hash '%s' from bob merkle trie for account %v", hex.EncodeToString(deleteHash), delta.address)
	//			} else {
	//				accumulatedChanges++
	//			}
	//		}
	//
	//		if !delta.newAcct.IsEmpty() {
	//			addHash := trackerdb.AccountHashBuilderV6(delta.address, &delta.newAcct, protocol.Encode(&delta.newAcct))
	//			added, err = ct.balancesTrie.Add(addHash)
	//			if err != nil {
	//				return fmt.Errorf("attempted to add duplicate hash '%s' to bob merkle trie for account %v: %w", hex.EncodeToString(addHash), delta.address, err)
	//			}
	//			if !added {
	//				ct.log.Errorf("attempted to add duplicate hash '%s' to bob merkle trie for account %v", hex.EncodeToString(addHash), delta.address)
	//			} else {
	//				accumulatedChanges++
	//			}
	//		}
	//	}
	//
	//	for i := 0; i < resourcesDeltas.len(); i++ {
	//		resDelta := resourcesDeltas.getByIdx(i)
	//		addr := resDelta.address
	//		if !resDelta.oldResource.Data.IsEmpty() {
	//			deleteHash, err := trackerdb.ResourcesHashBuilderV6(&resDelta.oldResource.Data, addr, resDelta.oldResource.Aidx, resDelta.oldResource.Data.UpdateRound, protocol.Encode(&resDelta.oldResource.Data))
	//			if err != nil {
	//				return err
	//			}
	//			deleted, err = ct.balancesTrie.Delete(deleteHash)
	//			if err != nil {
	//				return fmt.Errorf("failed to delete resource hash '%s' from bob merkle trie for account %v: %w", hex.EncodeToString(deleteHash), addr, err)
	//			}
	//			if !deleted {
	//				ct.log.Errorf("failed to delete resource hash '%s' from bob merkle trie for account %v", hex.EncodeToString(deleteHash), addr)
	//			} else {
	//				accumulatedChanges++
	//			}
	//		}
	//
	//		if !resDelta.newResource.IsEmpty() {
	//			addHash, err := trackerdb.ResourcesHashBuilderV6(&resDelta.newResource, addr, resDelta.oldResource.Aidx, resDelta.newResource.UpdateRound, protocol.Encode(&resDelta.newResource))
	//			if err != nil {
	//				return err
	//			}
	//			added, err = ct.balancesTrie.Add(addHash)
	//			if err != nil {
	//				return fmt.Errorf("attempted to add duplicate resource hash '%s' to bob merkle trie for account %v: %w", hex.EncodeToString(addHash), addr, err)
	//			}
	//			if !added {
	//				ct.log.Errorf("attempted to add duplicate resource hash '%s' to bob merkle trie for account %v", hex.EncodeToString(addHash), addr)
	//			} else {
	//				accumulatedChanges++
	//			}
	//		}
	//	}
	//
	//	for key, mv := range kvDeltas {
	//		if mv.oldData == nil && mv.data == nil { // Came and went within the delta span
	//			continue
	//		}
	//		if mv.oldData != nil {
	//			// reminder: check mv.data for nil here, b/c bytes.Equal conflates nil and "".
	//			if mv.data != nil && bytes.Equal(mv.oldData, mv.data) {
	//				continue // changed back within the delta span
	//			}
	//			deleteHash := trackerdb.KvHashBuilderV6(key, mv.oldData)
	//			deleted, err = ct.balancesTrie.Delete(deleteHash)
	//			if err != nil {
	//				return fmt.Errorf("failed to delete kv hash '%s' from bob merkle trie for key %v: %w", hex.EncodeToString(deleteHash), key, err)
	//			}
	//			if !deleted {
	//				ct.log.Errorf("failed to delete kv hash '%s' from bob merkle trie for key %v", hex.EncodeToString(deleteHash), key)
	//			} else {
	//				accumulatedChanges++
	//			}
	//		}
	//
	//		if mv.data != nil {
	//			addHash := trackerdb.KvHashBuilderV6(key, mv.data)
	//			added, err = ct.balancesTrie.Add(addHash)
	//			if err != nil {
	//				return fmt.Errorf("attempted to add duplicate kv hash '%s' from bob merkle trie for key %v: %w", hex.EncodeToString(addHash), key, err)
	//			}
	//			if !added {
	//				ct.log.Errorf("attempted to add duplicate kv hash '%s' from bob merkle trie for key %v", hex.EncodeToString(addHash), key)
	//			} else {
	//				accumulatedChanges++
	//			}
	//		}
	//	}

	// write it all to disk.
	//	var cstats bobtrie.CommitStats
	//	if accumulatedChanges > 0 {
	//		cstats, err = ct.balancesTrie.Commit()
	//_, err = ct.balancesTrie.Commit()
	//	}

	//root, rootErr := ct.balancesTrie.RootHash()
	//if rootErr != nil {
	//	ct.log.Errorf("accountsUpdateBalances: error retrieving balances trie root: %v", rootErr)
	//	return
	//}
	//	ct.log.Infof("accountsUpdateBalances: changes: %d root: %v", accumulatedChanges, root.String())
	//ct.log.Infof("accountsUpdateBalances: root: %v", root.String())
	return
}

// initializeHashes initializes account/resource/kv hashes.
// as part of the initialization, it tests if a hash table matches to account base and updates the former.
func (ct *bobTracker) initializeHashes(ctx context.Context, tx trackerdb.TransactionScope, rnd basics.Round) error {
	ct.log.Infof("bobtracker -- initialize hashes")
	ar, err := tx.MakeAccountsReader()
	if err != nil {
		return err
	}
    aw, err := tx.MakeAccountsWriter()
    if err != nil {
        return err
    }

	hashRound, err := ar.AccountsBobHashRound(ctx)
	if err != nil {
		return err
	}

	if hashRound != rnd {
		// if the hashed round is different then the base round, something was modified, and the accounts aren't in sync
		// with the hashes.
		err = aw.ResetAccountHashes(ctx)
		if err != nil {
			return err
		}
	}

	// create the bob merkle trie for the balances
	committer, err := tx.MakeBobCommitter(false)
	if err != nil {
		return fmt.Errorf("initializeHashes was unable to makeBobCommitter: %v", err)
	}

	trie, err := bobtrie.MakeTrie(committer, trackerdb.BobTrieMemoryConfig)
	if err != nil {
		return fmt.Errorf("initializeHashes was unable to MakeTrie: %v", err)
	}

	// we might have a database that was previously initialized, and now we're adding the balances trie. In that case, we need to add all the existing balances to this trie.
	// we can figure this out by examining the hash of the root:
	rootHash, err := trie.RootHash()
	if err != nil {
		return fmt.Errorf("initializeHashes was unable to retrieve trie root hash: %v", err)
	}

	if rootHash.IsZero() {
		ct.log.Infof("initializeHashes rebuilding bob merkle trie for round %d", rnd)
		accountBuilderIt := tx.MakeOrderedAccountsIter(bobtrieRebuildAccountChunkSize)
		defer accountBuilderIt.Close(ctx)
		startTrieBuildTime := time.Now()
		trieHashCount := 0
		lastRebuildTime := startTrieBuildTime
		pendingTrieHashes := 0
		totalOrderedAccounts := 0
        trie.BeginTransaction()
		for {
			accts, processedRows, err := accountBuilderIt.Next(ctx)
			if err == sql.ErrNoRows {
				// the account builder would return sql.ErrNoRows when no more data is available.
				break
			} else if err != nil {
                trie.RollbackTransaction()
				return err
			}

			if len(accts) > 0 {
				trieHashCount += len(accts)
				pendingTrieHashes += len(accts)
				for _, acct := range accts {
					added, err := trie.Add(acct.Digest)
					if err != nil {
                        trie.RollbackTransaction()
						return fmt.Errorf("initializeHashes was unable to add acct to trie: %v", err)
					}
					addr, err := ar.LookupAccountAddressFromAddressID(ctx, acct.AccountRef)
					ct.log.Warnf("initializeHashes add acct hash '%s' to bob merkle trie for account %v", hex.EncodeToString(acct.Digest), addr)
					if !added {
						// we need to translate the "addrid" into actual account address so that
						// we can report the failure.
						addr, err := ar.LookupAccountAddressFromAddressID(ctx, acct.AccountRef)
						if err != nil {
							ct.log.Warnf("initializeHashes attempted to add duplicate acct hash '%s' to bob merkle trie for account id %d : %v", hex.EncodeToString(acct.Digest), acct.AccountRef, err)
						} else {
							ct.log.Warnf("initializeHashes attempted to add duplicate acct hash '%s' to bob merkle trie for account %v", hex.EncodeToString(acct.Digest), addr)
						}
					}
				}

				if pendingTrieHashes >= bobtrieRebuildCommitFrequency {
					// this trie Evict will commit using the current transaction.
					// if anything goes wrong, it will still get rolled back.
					_, err = trie.Evict(true)
					if err != nil {
                        trie.RollbackTransaction()
						return fmt.Errorf("initializeHashes was unable to commit changes to trie: %v", err)
					}
					pendingTrieHashes = 0
				}

				if time.Since(lastRebuildTime) > 5*time.Second {
					// let the user know that the trie is still being rebuilt.
					ct.log.Infof("initializeHashes still building the trie, and processed so far %d trie entries", trieHashCount)
					lastRebuildTime = time.Now()
				}
			} else if processedRows > 0 {
				totalOrderedAccounts += processedRows
				// if it's not ordered, we can ignore it for now; we'll just increase the counters and emit logs periodically.
				if time.Since(lastRebuildTime) > 5*time.Second {
					// let the user know that the trie is still being rebuilt.
					ct.log.Infof("initializeHashes still building the bob trie, and hashed so far %d accounts", totalOrderedAccounts)
					lastRebuildTime = time.Now()
				}
			}
		}

		// this trie Evict will commit using the current transaction.
		// if anything goes wrong, it will still get rolled back.
		_, err = trie.Evict(true)
		if err != nil {
            trie.RollbackTransaction()
			return fmt.Errorf("initializeHashes was unable to commit changes to bob trie: %v", err)
		}

		// Now add the kvstore hashes
		pendingTrieHashes = 0
		kvs, err := tx.MakeKVsIter(ctx)
		if err != nil {
            trie.RollbackTransaction()
			return err
		}
		defer kvs.Close()
		for kvs.Next() {
			k, v, err := kvs.KeyValue()
			if err != nil {
                trie.RollbackTransaction()
				return err
			}
			hash := trackerdb.KvHashBuilderV6(string(k), v)
			trieHashCount++
			pendingTrieHashes++
			added, err := trie.Add(hash)
            ct.log.Warnf("initializeHashes add kv (key=%s) to bob trie", hex.EncodeToString(k))

			if err != nil {
                trie.RollbackTransaction()
				return fmt.Errorf("initializeHashes was unable to add kv (key=%s) to bob trie: %v", hex.EncodeToString(k), err)
			}
			if !added {
				ct.log.Warnf("initializeHashes attempted to add duplicate kv hash '%s' to bob merkle trie for key %s", hex.EncodeToString(hash), k)
			}
			if pendingTrieHashes >= trieRebuildCommitFrequency {
				// this trie Evict will commit using the current transaction.
				// if anything goes wrong, it will still get rolled back.
				_, err = trie.Evict(true)
				if err != nil {
                    trie.RollbackTransaction()
					return fmt.Errorf("initializeHashes was unable to commit changes to trie: %v", err)
				}
				pendingTrieHashes = 0
			}
			// We could insert code to report things every 5 seconds, like was done for accounts.
		}

		// this trie Evict will commit using the current transaction.
		// if anything goes wrong, it will still get rolled back.
		_, err = trie.Evict(true)
		if err != nil {
            trie.RollbackTransaction()
			return fmt.Errorf("initializeHashes was unable to commit changes to trie: %v", err)
		}

		// we've just updated the bob merkle trie, update the hashRound to reflect that.
		err = aw.UpdateAccountsBobHashRound(ctx, rnd)
		if err != nil {
            trie.RollbackTransaction()
			return fmt.Errorf("initializeHashes was unable to update the account hash round to %d: %v", rnd, err)
		}

        trie.CommitTransaction()
		ct.log.Infof("initializeHashes rebuilt the bob merkle trie with %d entries in %v", trieHashCount, time.Since(startTrieBuildTime))
	}
	ct.balancesTrie = trie
	return nil
}

func (ct *bobTracker) produceCommittingTask(committedRound basics.Round, dbRound basics.Round, dcr *deferredCommitRange) *deferredCommitRange {
	return dcr
}
