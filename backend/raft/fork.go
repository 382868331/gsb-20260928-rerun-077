package raft

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

// Fork phase values persisted in the manifest. They describe where the
// fork operation stopped, so any process restart can classify the pair
// without guessing from file presence alone.
const (
	// ForkPhasePartial: at least one side of the pair is missing, has a
	// corrupt record, or failed its own write. The operation is unfinished.
	ForkPhasePartial = "partial"
	// ForkPhaseRecovering: a repair is in progress: the missing side has
	// been rewritten but the pair was not yet revalidated/confirmed.
	ForkPhaseRecovering = "recovering"
	// ForkPhaseComplete: both sides are present, readable, replayable and
	// bind to the same frozen source snapshot.
	ForkPhaseComplete = "complete"
	// ForkPhaseCanceled: the unfinished fork was explicitly canceled. The
	// manifest remains only as an idempotency tombstone; retrying the
	// original request after cancellation starts a new operation identity.
	ForkPhaseCanceled = "canceled"
)

// ForkManifest is the single source of truth for one fork operation's
// identity. It binds the deterministic A/B branch ids to the exact frozen
// pause snapshot the request started from.
type ForkManifest struct {
	ForkKey     string   `json:"forkKey"`
	BranchAID   string   `json:"branchAId"`
	BranchBID   string   `json:"branchBId"`
	SourcePause Selector `json:"sourcePause"`
	SourceAt    int64    `json:"sourceAt"`
	// Base is the frozen common-prefix scenario both branches replay from.
	Base Scenario `json:"base"`
	// SourceHash is SHA-1 of the canonical base scenario. Each branch
	// record stores the same hash, so tampered or foreign records are
	// recognized instead of silently joining the pair.
	SourceHash string `json:"sourceHash"`
	Phase      string `json:"phase"`
	CreatedAt  string `json:"createdAt"`
	UpdatedAt  string `json:"updatedAt"`
}

// ForkRequestKey identifies a fork operation by its exact starting state
// (common-prefix scenario, pause selector and frozen clock). Repeating the
// same request — across retries or restarts — always maps to this key, so
// one pause point can never silently acquire several operation identities.
func ForkRequestKey(base Scenario, selector Selector, at int64) string {
	sum := sha1.Sum(append(append(MustJSON(base), MustJSON(selector)...), MustJSON(at)...))
	return hex.EncodeToString(sum[:])
}

// ScenarioHash fingerprints the frozen source snapshot used by a pair.
func ScenarioHash(base Scenario) string {
	sum := sha1.Sum(MustJSON(base))
	return hex.EncodeToString(sum[:])
}

// BranchID derives the branch record id from the operation key, so the
// missing side of a damaged pair is always rebuilt under the same id.
func BranchID(forkKey, name string) string {
	short := forkKey
	if len(short) > 16 {
		short = short[:16]
	}
	return fmt.Sprintf("exp-fork-%s-%s", short, name)
}

