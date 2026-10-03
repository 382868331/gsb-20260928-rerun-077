package main

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"raftlab/raft"
)

type Lab struct {
	mu          sync.Mutex
	sim         *raft.Simulator
	scenario    raft.Scenario
	experiments map[string]*experiment
	analyses    map[string]*raft.DivergenceAnalysis
	recoveries  map[string]*recoverySession
}

// recoverySession is a pending fork-pair recovery: the preview has been
// shown and the pair stays in "recovering" state until the user confirms
// (repair or cancel) with this token. Confirmations are idempotent per
// token, so a retried request cannot mint a second outcome.
type recoverySession struct {
	token     string
	forkKey   string
	createdAt string
	result    *recoveryResult
}

type experiment struct {
	doc     raft.ExperimentDoc
	sim     *raft.Simulator
	loadErr string
	file    string
}

const (
	experimentDir = "experiments"
	analysisDir   = "analyses"
)

type replayRequest struct {
	Scenario *raft.Scenario `json:"scenario"`
}

type saveRequest struct {
	Name     string         `json:"name"`
	Scenario *raft.Scenario `json:"scenario"`
}

func (lab *Lab) currentScenario() raft.Scenario {
	scenario := lab.scenario
	scenario.Actions = lab.sim.Actions()
	return scenario
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func decode(r *http.Request, value any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(value)
}

func recoverMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("handler panic on %s: %v", r.URL.Path, recovered)
				http.Error(w, "internal simulation error; workspace left unchanged", http.StatusInternalServerError)
			}
		}()
		next(w, r)
	}
}

func atomicWriteJSON(dir, name string, value any) (err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, name+".json")
	tmp, err := os.CreateTemp(dir, ".write-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err = tmp.Write(encoded); err != nil {
		return err
	}
	if err = tmp.Chmod(0o644); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func (lab *Lab) saveExperimentLocked(exp *experiment) error {
	if err := atomicWriteJSON(experimentDir, exp.doc.ID, exp.doc); err != nil {
		return err
	}
	exp.file = exp.doc.ID + ".json"
	return nil
}

func (lab *Lab) saveAnalysisLocked(analysis *raft.DivergenceAnalysis) error {
	return atomicWriteJSON(analysisDir, analysis.ID, analysis)
}

// forkRequestKey identifies one fork operation: the frozen pause state it
// started from (prefix actions, pause selector and clock). Retrying the
// same request maps to the same key, so no look-alike branches appear.
func forkRequestKey(base raft.Scenario, selector raft.Selector, at int64) string {
	sum := sha1.Sum(append(raft.MustJSON(base), raft.MustJSON(selector)...))
	return fmt.Sprintf("%x-%d", hex.EncodeToString(sum[:8]), at)
}

// Pair statuses: every fork operation owns exactly one identity (its
// forkKey), and the remnants on disk always resolve to one of these.
const (
	pairComplete   = "complete"   // both sides present and rebuildable
	pairPartial    = "partial"    // one side was never written (partial write)
	pairUnreadable = "unreadable" // one side's record is corrupt or unloadable
	pairRecovering = "recovering" // a recovery preview is pending confirmation
	pairCancelled  = "cancelled"  // incomplete fork cancelled; pause point released
)

type forkPairSide struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Healthy   bool   `json:"healthy"`
	LoadError string `json:"loadError,omitempty"`
	Decisions int    `json:"decisions"`
}

type forkPairView struct {
	ForkKey     string         `json:"forkKey"`
	Status      string         `json:"status"`
	SourcePause *raft.Selector `json:"sourcePause,omitempty"`
	SourceAt    int64          `json:"sourceAt,omitempty"`
	Sides       []forkPairSide `json:"sides"`
	Missing     string         `json:"missing,omitempty"`
}

// forkPairStatusLocked resolves one fork identity to its current persisted
// state, including corrupt and half-written remnants. It returns nil when
// the pause point has never produced (or no longer claims) a pair.
func (lab *Lab) forkPairStatusLocked(key string) *forkPairView {
	view := &forkPairView{ForkKey: key, Status: pairComplete, Sides: []forkPairSide{}}
	var healthyA, healthyB *experiment
	broken := map[string]*experiment{}
	cancelled := false
	for _, exp := range lab.experiments {
		if exp.doc.ForkKey != key {
			continue
		}
		if exp.doc.PairCancelled {
			cancelled = true
		}
		name := exp.doc.Name
		if name != "a" && name != "b" {
			name = "?"
		}
		side := forkPairSide{ID: exp.doc.ID, Name: name, Healthy: exp.loadErr == "", LoadError: exp.loadErr, Decisions: len(exp.doc.Decisions)}
		view.Sides = append(view.Sides, side)
		if view.SourcePause == nil && exp.doc.SourcePause != nil {
			view.SourcePause = exp.doc.SourcePause
			view.SourceAt = exp.doc.SourceAt
		}
		if exp.loadErr != "" {
			broken[name] = exp
			continue
		}
		switch name {
		case "a":
			healthyA = exp
		case "b":
			healthyB = exp
		}
	}
	if len(view.Sides) == 0 {
		return nil
	}
	sort.Slice(view.Sides, func(i, j int) bool { return view.Sides[i].ID < view.Sides[j].ID })
	switch {
	case cancelled:
		view.Status = pairCancelled
	case healthyA != nil && healthyB != nil:
		view.Status = pairComplete
	case lab.recoveries != nil && lab.recoveries[key] != nil && lab.recoveries[key].result == nil:
		view.Status = pairRecovering
	case len(broken) > 0:
		view.Status = pairUnreadable
	default:
		view.Status = pairPartial
	}
	if healthyA == nil {
		view.Missing = "a"
	}
	if healthyB == nil {
		view.Missing = "b"
	}
	return view
}

// listForkPairsLocked groups every experiment that carries a fork identity
// into auditable pair views, including damaged and cancelled ones.
func (lab *Lab) listForkPairsLocked() []forkPairView {
	keys := map[string]bool{}
	for _, exp := range lab.experiments {
		if exp.doc.ForkKey != "" {
			keys[exp.doc.ForkKey] = true
		}
	}
	pairs := []forkPairView{}
	for key := range keys {
		if view := lab.forkPairStatusLocked(key); view != nil {
			pairs = append(pairs, *view)
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].ForkKey < pairs[j].ForkKey })
	return pairs
}

type recoveryPreview struct {
	Pair          forkPairView `json:"pair"`
	Token         string       `json:"token"`
	KeepSide      string       `json:"keepSide"`
	KeepDecisions int          `json:"keepDecisions"`
	RepairSide    string       `json:"repairSide"`
	RepairID      string       `json:"repairId"`
	Actions       []string     `json:"actions"`
	Summary       string       `json:"summary"`
}

type recoveryResult struct {
	Pair     forkPairView    `json:"pair"`
	Repaired *experimentView `json:"repaired,omitempty"`
	Message  string          `json:"message"`
}

// healthySideLocked returns the rebuildable a/b side of a pair, if any.
func (lab *Lab) healthySideLocked(key string) *experiment {
	for _, exp := range lab.experiments {
		if exp.doc.ForkKey == key && exp.loadErr == "" && !exp.doc.PairCancelled &&
			(exp.doc.Name == "a" || exp.doc.Name == "b") {
			return exp
		}
	}
	return nil
}

// buildPreviewLocked describes the planned recovery without changing
// anything: which side and how many recorded decisions are preserved, and
// which side identity a repair would recreate.
func (lab *Lab) buildPreviewLocked(pair *forkPairView, token string) recoveryPreview {
	preview := recoveryPreview{Pair: *pair, Token: token, Actions: []string{"repair", "cancel"}}
	keep := lab.healthySideLocked(pair.ForkKey)
	if keep != nil {
		preview.KeepSide = keep.doc.ID
		preview.KeepDecisions = len(keep.doc.Decisions)
	}
	missing := pair.Missing
	if missing == "" {
		missing = "b"
	}
	preview.RepairSide = missing
	preview.RepairID = lab.repairIDLocked(pair, missing)
	preview.Summary = fmt.Sprintf("保留侧 %s（已记录 %d 条决定，历史不变）；修复将按共同前缀与来源暂停状态重建 %s 侧为 %s，或取消该分叉并保留现有分支。",
		preview.KeepSide, preview.KeepDecisions, missing, preview.RepairID)
	return preview
}

// repairIDLocked picks the deterministic identity for the recreated side:
// the corrupt record's own id when one exists, otherwise a stable id
// derived from the fork key, so repeated repairs converge on one record.
func (lab *Lab) repairIDLocked(pair *forkPairView, side string) string {
	for _, exp := range lab.experiments {
		if exp.doc.ForkKey == pair.ForkKey && exp.doc.Name == side && exp.loadErr != "" {
			return strings.TrimSuffix(exp.file, ".json")
		}
	}
	return "exp-" + pair.ForkKey + "-" + side
}

// repairPairLocked recreates the missing or unreadable side of a pair from
// the surviving side's persisted base scenario and source pause state. The
// surviving branch is never touched, so its executed history and the shared
// prefix stay exactly as recorded.
func (lab *Lab) repairPairLocked(key string) (*recoveryResult, error) {
	pair := lab.forkPairStatusLocked(key)
	if pair == nil {
		return nil, fmt.Errorf("配对不存在")
	}
	if pair.Status == pairComplete {
		return &recoveryResult{Pair: *pair, Message: "配对已完整，无需修复"}, nil
	}
	keep := lab.healthySideLocked(key)
	if keep == nil {
		return nil, fmt.Errorf("两侧记录都无法重建，缺少可核对的来源快照，不能修复")
	}
	side := pair.Missing
	if side == "" {
		side = "b"
	}
	repairID := lab.repairIDLocked(pair, side)
	if existing := lab.experiments[repairID]; existing != nil && existing.loadErr == "" {
		// A previous repair already landed: report it instead of duplicating.
		fresh := lab.forkPairStatusLocked(key)
		return &recoveryResult{Pair: *fresh, Message: "该侧已修复，重复请求未产生新记录"}, nil
	}
	doc := raft.ExperimentDoc{
		ID:          repairID,
		Name:        side,
		Base:        keep.doc.Base,
		Decisions:   []raft.Decision{},
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		SourcePause: keep.doc.SourcePause,
		SourceAt:    keep.doc.SourceAt,
		ForkKey:     key,
	}
	sim, err := raft.LoadExperiment(doc)
	if err != nil {
		return nil, fmt.Errorf("无法按共同前缀重建缺失侧：%v", err)
	}
	repaired := &experiment{doc: doc, sim: sim}
	if err := lab.saveExperimentLocked(repaired); err != nil {
		return nil, fmt.Errorf("修复侧保存失败，现有记录未改动：%v", err)
	}
	// Drop the damaged remnants of the repaired side (files and memory);
	// every other experiment, including the surviving branch, is untouched.
	for id, exp := range lab.experiments {
		if exp.doc.ForkKey == key && exp.doc.Name == side && exp.loadErr != "" {
			if exp.file != "" && exp.file != repaired.file {
				_ = os.Remove(filepath.Join(experimentDir, exp.file))
			}
			delete(lab.experiments, id)
		}
	}
	lab.experiments[doc.ID] = repaired
	fresh := lab.forkPairStatusLocked(key)
	view := repaired.view()
	return &recoveryResult{Pair: *fresh, Repaired: &view, Message: fmt.Sprintf("已修复 %s 侧为 %s；保留侧 %s 的 %d 条决定与共同前缀均未改动。", side, doc.ID, keep.doc.ID, len(keep.doc.Decisions))}, nil
}

// cancelPairLocked dissolves an incomplete fork: the surviving branch keeps
// its full recorded history and is only marked as no longer claiming the
// pause point, so a later fork there gets a fresh, explicit identity.
func (lab *Lab) cancelPairLocked(key string) (*recoveryResult, error) {
	pair := lab.forkPairStatusLocked(key)
	if pair == nil {
		return nil, fmt.Errorf("配对不存在")
	}
	if pair.Status == pairCancelled {
		return &recoveryResult{Pair: *pair, Message: "该分叉已取消，重复请求未改动任何记录"}, nil
	}
	if pair.Status == pairComplete {
		return nil, fmt.Errorf("配对完整，不能取消；如需重来请直接在新暂停点分叉")
	}
	keep := lab.healthySideLocked(key)
	if keep != nil {
		keep.doc.PairCancelled = true
		if err := lab.saveExperimentLocked(keep); err != nil {
			return nil, fmt.Errorf("取消标记保存失败，现有记录未改动：%v", err)
		}
	}
	// Remove only the unreadable remnants of the lost side; the surviving
	// branch file and its decisions stay byte-identical except the flag.
	for id, exp := range lab.experiments {
		if exp.doc.ForkKey == key && exp.loadErr != "" {
			if exp.file != "" {
				_ = os.Remove(filepath.Join(experimentDir, exp.file))
			}
			delete(lab.experiments, id)
		}
	}
	fresh := lab.forkPairStatusLocked(key)
	if fresh == nil {
		fresh = &forkPairView{ForkKey: key, Status: pairCancelled, Sides: []forkPairSide{}}
	}
	return &recoveryResult{Pair: *fresh, Message: "已取消该未完成分叉；保留分支历史不变，该暂停点可重新分叉。"}, nil
}

func (lab *Lab) loadExperiments() {
	lab.experiments = map[string]*experiment{}
	entries, err := os.ReadDir(experimentDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(experimentDir, entry.Name()))
		if err != nil {
			continue
		}
		var doc raft.ExperimentDoc
		if err := json.Unmarshal(raw, &doc); err != nil {
			log.Printf("keep unreadable experiment %s: %v", entry.Name(), err)
			broken := raft.ExperimentDoc{ID: entry.Name(), Name: "unreadable"}
			// Best-effort identity recovery: even a corrupt record still
			// claims its fork pair, so the pause point cannot silently
			// mint a second pair while this remnant exists.
			if id, name, key := extractForkIdentity(raw); key != "" {
				broken.ForkKey = key
				if name == "a" || name == "b" {
					broken.Name = name
				}
				if id != "" {
					broken.ID = id
				}
			}
			lab.experiments[entry.Name()] = &experiment{
				doc:     broken,
				file:    entry.Name(),
				loadErr: fmt.Sprintf("实验记录 JSON 已损坏：%v；其他实验仍保留。", err),
			}
			continue
		}
		sim, err := raft.LoadExperiment(doc)
		if err != nil {
			log.Printf("keep unloadable experiment %s: %v", entry.Name(), err)
			lab.experiments[doc.ID] = &experiment{doc: doc, file: entry.Name(), loadErr: fmt.Sprintf("实验无法重建：%v；已保留暂停点和已保存操作。", err)}
			continue
		}
		lab.experiments[doc.ID] = &experiment{doc: doc, sim: sim, file: entry.Name()}
	}
}

var (
	forkIDPattern   = regexp.MustCompile(`"id"\s*:\s*"([^"]+)"`)
	forkNamePattern = regexp.MustCompile(`"name"\s*:\s*"([ab])"`)
	forkKeyPattern  = regexp.MustCompile(`"forkKey"\s*:\s*"([^"]+)"`)
)

// extractForkIdentity salvages the pair identity fields from a record
// whose JSON no longer parses, so a corrupt file still counts as a
// remnant of its fork operation instead of an invisible orphan.
func extractForkIdentity(raw []byte) (id, name, key string) {
	if match := forkIDPattern.FindSubmatch(raw); match != nil {
		id = string(match[1])
	}
	if match := forkNamePattern.FindSubmatch(raw); match != nil {
		name = string(match[1])
	}
	if match := forkKeyPattern.FindSubmatch(raw); match != nil {
		key = string(match[1])
	}
	return id, name, key
}

func (lab *Lab) loadAnalyses() {
	lab.analyses = map[string]*raft.DivergenceAnalysis{}
	entries, err := os.ReadDir(analysisDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(analysisDir, entry.Name()))
		if err != nil {
			continue
		}
		var analysis raft.DivergenceAnalysis
		if err := json.Unmarshal(raw, &analysis); err != nil {
			log.Printf("skip broken analysis %s: %v", entry.Name(), err)
			continue
		}
		lab.analyses[analysis.ID] = &analysis
	}
}

type experimentView struct {
	raft.ExperimentDoc
	State     *raft.State `json:"state"`
	LoadError string      `json:"loadError,omitempty"`
}

func (exp *experiment) view() experimentView {
	view := experimentView{ExperimentDoc: exp.doc, LoadError: exp.loadErr}
	if view.Decisions == nil {
		// Never emit null: the UI reads decisions as a list and a null
		// here used to crash the whole comparison region.
		view.Decisions = []raft.Decision{}
	}
	if exp.sim != nil {
		state := exp.sim.Snapshot(300)
		view.State = &state
	}
	return view
}

// recordDecision captures simulator actions appended since before and adds
// them to the branch's auditable decision history.
func (exp *experiment) recordDecision(kind, label string, before int) bool {
	actions := exp.sim.Actions()
	fresh := append([]raft.Action(nil), actions[before:]...)
	if len(fresh) == 0 {
		return false
	}
	exp.doc.Decisions = append(exp.doc.Decisions, raft.Decision{
		ID:      "d" + strconv.Itoa(len(exp.doc.Decisions)+1),
		Kind:    kind,
		Label:   label,
		At:      fresh[0].At,
		Actions: fresh,
	})
	return true
}

func findIntervention(doc raft.ExperimentDoc, ref raft.InterventionRef) (raft.Action, error) {
	for _, decision := range doc.Decisions {
		if decision.ID != ref.DecisionID {
			continue
		}
		if ref.ActionIndex < 0 || ref.ActionIndex >= len(decision.Actions) {
			return raft.Action{}, fmt.Errorf("intervention action index out of range")
		}
		return decision.Actions[ref.ActionIndex], nil
	}
	return raft.Action{}, fmt.Errorf("intervention decision not found")
}

func isNetworkIntervention(action raft.Action) bool {
	switch action.Op {
	case raft.ActionDeliverFirst, raft.ActionPostpone, "delay", "link", "partition", "heal":
		return true
	default:
		return false
	}
}

func isReorderDecision(decision raft.Decision) bool {
	for _, action := range decision.Actions {
		if action.Op != raft.ActionDeliverFirst && action.Op != raft.ActionPostpone {
			return false
		}
	}
	return len(decision.Actions) > 0
}

func messageForDecision(decision raft.Decision, sim *raft.Simulator) *raft.Message {
	for _, action := range decision.Actions {
		message := raft.ActionMessage(action)
		if message == nil {
			continue
		}
		if found := raft.FindQueuedMessage(sim, raft.Selector{Type: message.Type, From: message.From, To: message.To, Term: message.Term, Data: message.Data}); found != nil {
			return found
		}
	}
	return nil
}

func messageForPair(doc raft.ExperimentDoc, decision raft.Decision) *raft.Message {
	sim, err := raft.LoadExperimentBeforeDecision(doc, decision.ID)
	if err != nil {
		return nil
	}
	return messageForDecision(decision, sim)
}

func (lab *Lab) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/advance", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Ticks int64 `json:"ticks"`
		}
		if err := decode(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		start := lab.sim.Now()
		lab.sim.Advance(body.Ticks)
		lab.sim.RecordAdvance(start, lab.sim.Now())
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/client", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Node int
			Data string
		}
		if err := decode(r, &body); err != nil || body.Data == "" {
			http.Error(w, "node and data required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		if err := lab.sim.Client(body.Node, body.Data); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			lab.mu.Unlock()
			return
		}
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/link", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			From, To int
			Up       *bool
			Delay    *int64
		}
		if err := decode(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		if body.Up != nil {
			lab.sim.SetLink(body.From, body.To, *body.Up)
		}
		if body.Delay != nil {
			_ = lab.sim.SetDelay(body.From, body.To, *body.Delay)
		}
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/partition", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groups [][]int `json:"groups"`
		}
		if err := decode(r, &body); err != nil || len(body.Groups) == 0 {
			http.Error(w, "groups required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		lab.sim.Partition(body.Groups)
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/heal", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		lab.sim.HealAll()
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/replay", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body replayRequest
		if err := decode(r, &body); err != nil || body.Scenario == nil {
			http.Error(w, "scenario required", http.StatusBadRequest)
			return
		}
		// Build on a scratch simulator first: a broken replay must never
		// replace (and therefore destroy) the currently open workspace.
		candidate := raft.Replay(*body.Scenario)
		candidate.Snapshot(0)
		lab.mu.Lock()
		lab.scenario = *body.Scenario
		lab.sim = candidate
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("GET /api/scenarios", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		entries, _ := os.ReadDir("scenarios")
		names := []string{"minority-cannot-commit", "old-leader-catches-up"}
		for _, entry := range entries {
			if !entry.IsDir() {
				names = append(names, entry.Name())
			}
		}
		sort.Strings(names[2:])
		writeJSON(w, names)
	}))
	mux.HandleFunc("GET /api/scenario", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		scenario := lab.currentScenario()
		lab.mu.Unlock()
		writeJSON(w, scenario)
	}))
	mux.HandleFunc("POST /api/save", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body saveRequest
		if err := decode(r, &body); err != nil || body.Scenario == nil || body.Name == "" {
			http.Error(w, "name and scenario required", http.StatusBadRequest)
			return
		}
		scenario := lab.currentScenario()
		scenario.Name = body.Name
		lab.scenario = scenario
		if err := os.MkdirAll("scenarios", 0o755); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		encoded, err := json.MarshalIndent(scenario, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile("scenarios/"+body.Name+".json", encoded, 0o644); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]bool{"saved": true})
	}))
	mux.HandleFunc("POST /api/arm-pause", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Selector raft.Selector `json:"selector"`
		}
		if err := decode(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		lab.sim.ArmPause(body.Selector)
		lab.sim.Advance(1 << 40)
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/clear-pause", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		lab.sim.ClearPause()
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/fork", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Selector raft.Selector `json:"selector"`
			NameA    string        `json:"nameA"`
			NameB    string        `json:"nameB"`
		}
		if err := decode(r, &body); err != nil || body.Selector.Type == "" {
			http.Error(w, "selector required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		if !lab.sim.Paused() {
			lab.mu.Unlock()
			http.Error(w, "no active pause point; arm one before forking", http.StatusBadRequest)
			return
		}
		base := lab.currentScenario()
		base.Actions = append(base.Actions, raft.Action{
			Op: "advance", At: lab.sim.Now(), Delay: 1 << 40,
		})
		forkKey := forkRequestKey(base, body.Selector, lab.sim.Now())
		// A cancelled pair no longer claims its identity: the next fork at
		// the same pause point gets the next generation of the key, so old
		// and new operations never share an identity silently. The mapping
		// is deterministic given the persisted records, so restarts and
		// retries still converge on the same key.
		for generation := 2; ; generation++ {
			pair := lab.forkPairStatusLocked(forkKey)
			if pair == nil {
				break
			}
			if pair.Status != pairCancelled {
				break
			}
			forkKey = fmt.Sprintf("%s#g%d", forkRequestKey(base, body.Selector, lab.sim.Now()), generation)
		}
		if pair := lab.forkPairStatusLocked(forkKey); pair != nil {
			// This pause point already owns a fork identity. A complete
			// pair is returned as-is (idempotent retry); any damaged or
			// half-written remnant blocks silent re-forking and must go
			// through the explicit recovery flow instead.
			if pair.Status == pairComplete {
				pairA := lab.healthySideLocked(forkKey)
				var pairB *experiment
				for _, exp := range lab.experiments {
					if exp.doc.ForkKey == forkKey && exp.loadErr == "" && exp != pairA {
						pairB = exp
					}
				}
				viewA := pairA.view()
				viewB := pairB.view()
				lab.mu.Unlock()
				if pairA.doc.Name == "a" {
					writeJSON(w, map[string]experimentView{"a": viewA, "b": viewB})
				} else {
					writeJSON(w, map[string]experimentView{"a": viewB, "b": viewA})
				}
				return
			}
			lab.mu.Unlock()
			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]any{
				"error": "该暂停点已有一对分叉（状态：" + pair.Status + "），不能静默生成第二对；请在配对恢复中选择修复缺失侧或取消该分叉。",
				"pair":  pair,
			})
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		makeBranch := func(name string) *experiment {
			stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
			id := "exp-" + stamp + "-" + name
			for suffix := 1; lab.experiments[id] != nil; suffix++ {
				id = fmt.Sprintf("exp-%s-%s-%d", stamp, name, suffix)
			}
			sourcePause := body.Selector
			doc := raft.ExperimentDoc{
				ID:          id,
				Name:        name,
				Base:        base,
				Decisions:   []raft.Decision{},
				CreatedAt:   now,
				SourcePause: &sourcePause,
				SourceAt:    lab.sim.Now(),
				ForkKey:     forkKey,
			}
			// Clone the live simulator at the pause point: both branches share
			// the exact same prefix and then diverge independently.
			sim := lab.sim.Clone()
			exp := &experiment{doc: doc, sim: sim}
			lab.experiments[doc.ID] = exp
			return exp
		}
		branchA := makeBranch("a")
		branchB := makeBranch("b")
		if err := lab.saveExperimentLocked(branchA); err != nil {
			delete(lab.experiments, branchA.doc.ID)
			delete(lab.experiments, branchB.doc.ID)
			lab.mu.Unlock()
			http.Error(w, "分叉 A 保存失败，未保留半成品："+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := lab.saveExperimentLocked(branchB); err != nil {
			_ = os.Remove(filepath.Join(experimentDir, branchA.doc.ID+".json"))
			delete(lab.experiments, branchA.doc.ID)
			delete(lab.experiments, branchB.doc.ID)
			lab.mu.Unlock()
			http.Error(w, "分叉 B 保存失败，已回滚 A，现有实验未改动："+err.Error(), http.StatusInternalServerError)
			return
		}
		viewA := branchA.view()
		viewB := branchB.view()
		lab.mu.Unlock()
		writeJSON(w, map[string]experimentView{"a": viewA, "b": viewB})
	}))
	mux.HandleFunc("GET /api/experiments", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		views := []experimentView{}
		for _, exp := range lab.experiments {
			views = append(views, exp.view())
		}
		lab.mu.Unlock()
		sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
		writeJSON(w, views)
	}))
	mux.HandleFunc("GET /api/experiments/", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/api/experiments/"):]
		lab.mu.Lock()
		exp, ok := lab.experiments[id]
		var view experimentView
		if ok {
			view = exp.view()
		}
		lab.mu.Unlock()
		if !ok {
			http.Error(w, "experiment not found", http.StatusNotFound)
			return
		}
		writeJSON(w, view)
	}))
	mux.HandleFunc("GET /api/fork-pairs", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		pairs := lab.listForkPairsLocked()
		lab.mu.Unlock()
		writeJSON(w, pairs)
	}))
	mux.HandleFunc("POST /api/fork-recovery/preview", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ForkKey string `json:"forkKey"`
		}
		if err := decode(r, &body); err != nil || body.ForkKey == "" {
			http.Error(w, "forkKey required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		pair := lab.forkPairStatusLocked(body.ForkKey)
		if pair == nil {
			http.Error(w, "未找到该分叉配对", http.StatusNotFound)
			return
		}
		if pair.Status == pairComplete || pair.Status == pairCancelled {
			http.Error(w, "该配对无需恢复（当前状态："+pair.Status+"）", http.StatusConflict)
			return
		}
		// Re-requesting a preview for a pair already in recovery returns the
		// same pending session instead of stacking a second operation.
		if session := lab.recoveries[body.ForkKey]; session != nil && session.result == nil {
			pair.Status = pairRecovering
			writeJSON(w, lab.buildPreviewLocked(pair, session.token))
			return
		}
		tokenBytes := make([]byte, 8)
		if _, err := rand.Read(tokenBytes); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		token := hex.EncodeToString(tokenBytes)
		if lab.recoveries == nil {
			lab.recoveries = map[string]*recoverySession{}
		}
		lab.recoveries[body.ForkKey] = &recoverySession{token: token, forkKey: body.ForkKey, createdAt: time.Now().UTC().Format(time.RFC3339)}
		pair.Status = pairRecovering
		writeJSON(w, lab.buildPreviewLocked(pair, token))
	}))
	mux.HandleFunc("POST /api/fork-recovery/confirm", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ForkKey string `json:"forkKey"`
			Token   string `json:"token"`
			Action  string `json:"action"` // "repair" or "cancel"
		}
		if err := decode(r, &body); err != nil || body.ForkKey == "" || body.Token == "" {
			http.Error(w, "forkKey、token 和 action 均为必填", http.StatusBadRequest)
			return
		}
		if body.Action != "repair" && body.Action != "cancel" {
			http.Error(w, "action 只能是 repair 或 cancel", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		session := lab.recoveries[body.ForkKey]
		if session == nil || session.token != body.Token {
			http.Error(w, "恢复会话不存在或已失效，请重新预览", http.StatusConflict)
			return
		}
		// Duplicate confirm with the same token replays the recorded outcome
		// instead of applying the recovery a second time.
		if session.result != nil {
			writeJSON(w, session.result)
			return
		}
		var result *recoveryResult
		var err error
		if body.Action == "repair" {
			result, err = lab.repairPairLocked(body.ForkKey)
		} else {
			result, err = lab.cancelPairLocked(body.ForkKey)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		session.result = result
		writeJSON(w, result)
	}))
	mux.HandleFunc("GET /api/analyses", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		out := make([]raft.DivergenceAnalysis, 0, len(lab.analyses))
		for _, analysis := range lab.analyses {
			out = append(out, *analysis)
		}
		lab.mu.Unlock()
		sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
		writeJSON(w, out)
	}))
	mux.HandleFunc("POST /api/analyses", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ExperimentAID string               `json:"experimentAId"`
			ExperimentBID string               `json:"experimentBId"`
			Candidate     raft.InterventionRef `json:"candidate"`
		}
		if err := decode(r, &body); err != nil || body.ExperimentAID == "" || body.ExperimentBID == "" || body.Candidate.ExperimentID == "" || body.Candidate.DecisionID == "" {
			http.Error(w, "experimentAId、experimentBId 和 candidate.decisionId 均为必填", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		expA, okA := lab.experiments[body.ExperimentAID]
		expB, okB := lab.experiments[body.ExperimentBID]
		if !okA || !okB || expA.sim == nil || expB.sim == nil {
			http.Error(w, "实验轨迹缺失或无法重建，不能创建分析；已有记录未改动", http.StatusBadRequest)
			return
		}
		decisionA, decisionB, firstIndex := raft.FirstDifferentDecision(expA.doc, expB.doc)
		if firstIndex < 0 {
			http.Error(w, "两条轨迹尚未出现首次决定分歧", http.StatusBadRequest)
			return
		}
		treated := expA
		opposite := expB
		firstOnTreated := decisionA
		firstOnOpposite := decisionB
		if body.Candidate.ExperimentID == expB.doc.ID {
			treated, opposite = expB, expA
			firstOnTreated, firstOnOpposite = decisionB, decisionA
		} else if body.Candidate.ExperimentID != expA.doc.ID {
			http.Error(w, "所选干预不属于 A/B 中任何一条轨迹", http.StatusBadRequest)
			return
		}
		action, actionErr := findIntervention(treated.doc, body.Candidate)
		classification := "incomplete"
		explanation := "资料尚未完整：选择的干预不在首次决定分歧中，不能据此归因。"
		sufficient, complete := false, false
		var controlDoc raft.ExperimentDoc
		var controlSim, treatedAt, oppositeAt *raft.Simulator
		var messageTreated, messageOpposite *raft.Message
		if body.Candidate.DecisionID == firstOnTreated.ID && isNetworkIntervention(action) && actionErr == nil {
			var err error
			controlDoc, _, err = raft.ExperimentWithoutAction(treated.doc, body.Candidate)
			if err != nil {
				http.Error(w, "建立对照失败："+err.Error(), http.StatusBadRequest)
				return
			}
			controlSim, err = raft.LoadExperimentControlAtDecision(controlDoc, body.Candidate.DecisionID, body.Candidate.ActionIndex)
			if err != nil {
				http.Error(w, "对照无法推进到对应消息："+err.Error(), http.StatusBadRequest)
				return
			}
			treatedAt, err = raft.LoadExperimentAtDecision(treated.doc, firstOnTreated.ID)
			if err != nil {
				http.Error(w, "干预轨迹无法重建到首次分歧："+err.Error(), http.StatusBadRequest)
				return
			}
			oppositeAt, err = raft.LoadExperimentAtDecision(opposite.doc, firstOnOpposite.ID)
			if err != nil {
				http.Error(w, "对照轨迹无法重建到首次分歧："+err.Error(), http.StatusBadRequest)
				return
			}
			treatedPre, err := raft.LoadExperimentBeforeDecision(treated.doc, firstOnTreated.ID)
			if err != nil {
				http.Error(w, "干预轨迹无法定位首次分歧消息："+err.Error(), http.StatusBadRequest)
				return
			}
			oppositePre, err := raft.LoadExperimentBeforeDecision(opposite.doc, firstOnOpposite.ID)
			if err != nil {
				http.Error(w, "对照轨迹无法定位首次分歧消息："+err.Error(), http.StatusBadRequest)
				return
			}
			messageTreated = messageForDecision(firstOnTreated, treatedPre)
			messageOpposite = messageForDecision(firstOnOpposite, oppositePre)
			if messageTreated == nil || messageOpposite == nil {
				classification = "incomplete"
				explanation = "资料尚未完整：首次分歧对应的在途消息已找不到，不能判断是否只是到达顺序不同。"
			} else {
				complete = true
				treatedState := treatedAt.Snapshot(0)
				oppositeState := oppositeAt.Snapshot(0)
				treatedPreState := treatedPre.Snapshot(0)
				oppositePreState := oppositePre.Snapshot(0)
				controlState := controlSim.Snapshot(0)
				if raft.NodeRecordsDiffer(treatedPreState, oppositePreState) {
					classification = "different-acknowledgements"
					explanation = "节点已作出不同确认：首次决定时角色、任期、投票或本地提交记录已经不同。"
				} else if isReorderDecision(firstOnTreated) {
					classification = "arrival-order-only"
					explanation = "仅消息到达顺序不同：节点确认记录尚未分叉。"
				} else {
					classification = "incomplete"
					explanation = "资料尚未完整：当前差异不是可回放的消息顺序或节点确认差异。"
					complete = false
				}
				sufficient = complete && !raft.ObservedStateDiffer(controlState, oppositeState) && raft.ObservedStateDiffer(treatedState, controlState)
				if complete && sufficient {
					explanation += " 移除所选干预后的对照与另一轨迹一致，该干预足以解释本次目标分歧。"
				} else if complete {
					explanation += " 移除所选干预后仍不能得到另一轨迹，单靠该干预不足以解释分歧，不能归因给最后一次操作。"
				}
			}
			if existing := lab.experiments[controlDoc.ID]; existing == nil {
				controlExp := &experiment{doc: controlDoc, sim: controlSim}
				if err := lab.saveExperimentLocked(controlExp); err != nil {
					http.Error(w, "对照保存失败，分析未写入："+err.Error(), http.StatusInternalServerError)
					return
				}
				lab.experiments[controlDoc.ID] = controlExp
			}
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		id := "analysis-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		for lab.analyses[id] != nil {
			id += "-" + strconv.Itoa(len(id))
		}
		var pausePoint *raft.Selector
		if actions := treated.doc.Base.Actions; len(actions) > 0 {
			for _, action := range actions {
				if action.Op == raft.ActionPause {
					sel := raft.Selector{Type: raft.MessageType(action.MsgType), From: action.From, To: action.To, Term: action.Term, Data: action.Data}
					pausePoint = &sel
					break
				}
			}
		}
		analysis := &raft.DivergenceAnalysis{
			ID: id, ExperimentAID: body.ExperimentAID, ExperimentBID: body.ExperimentBID, CreatedAt: now,
			PausePoint: pausePoint, Candidate: body.Candidate, ControlID: controlDoc.ID,
			Classification: classification, Sufficient: sufficient, Complete: complete, Explanation: explanation,
			FirstDecisionA: decisionA, FirstDecisionB: decisionB, MessageA: messageForPair(expA.doc, decisionA),
			MessageB: messageForPair(expB.doc, decisionB), ControlDoc: controlDoc, TreatedDoc: treated.doc,
		}
		if controlSim != nil {
			state := controlSim.Snapshot(300)
			analysis.ControlState = &state
		}
		if treatedAt != nil {
			state := treatedAt.Snapshot(300)
			analysis.TreatedState = &state
		}
		if oppositeAt != nil {
			state := oppositeAt.Snapshot(300)
			analysis.OppositeState = &state
		}
		if err := lab.saveAnalysisLocked(analysis); err != nil {
			http.Error(w, "分析保存失败："+err.Error(), http.StatusInternalServerError)
			return
		}
		lab.analyses[id] = analysis
		writeJSON(w, analysis)
	}))
	// mutateBranch runs one audited experiment decision: it snapshots the
	// action log length, applies the request, persists the new decision and
	// returns the updated view. Failures persist nothing.
	mutateBranch := func(w http.ResponseWriter, r *http.Request, mutate func(r *http.Request, exp *experiment) (string, error)) {
		id := r.PathValue("id")
		lab.mu.Lock()
		exp, ok := lab.experiments[id]
		if !ok {
			lab.mu.Unlock()
			http.Error(w, "experiment not found", http.StatusNotFound)
			return
		}
		if exp.sim == nil || exp.loadErr != "" {
			lab.mu.Unlock()
			http.Error(w, "该实验记录无法重建，已保留为只读，不能继续推进", http.StatusBadRequest)
			return
		}
		if exp.doc.ReadOnly {
			lab.mu.Unlock()
			http.Error(w, "这是已保存分析的只读对照，继续实验请从原轨迹重新分叉", http.StatusBadRequest)
			return
		}
		before := len(exp.sim.Actions())
		label, err := mutate(r, exp)
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !exp.recordDecision(label, label, before) {
			lab.mu.Unlock()
			http.Error(w, "该操作没有产生可记录的实验决定，未保存", http.StatusBadRequest)
			return
		}
		if err := lab.saveExperimentLocked(exp); err != nil {
			lab.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		view := exp.view()
		lab.mu.Unlock()
		writeJSON(w, view)
	}
	mux.HandleFunc("POST /api/experiments/{id}/advance", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Ticks int64 `json:"ticks"`
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			start := exp.sim.Now()
			exp.sim.Advance(body.Ticks)
			exp.sim.RecordAdvance(start, exp.sim.Now())
			return "advance", nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/client", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Node int
				Data string
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			if err := exp.sim.Client(body.Node, body.Data); err != nil {
				return "", err
			}
			return "client:" + body.Data, nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/link", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				From, To int
				Up       *bool
				Delay    *int64
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			label := "link"
			if body.Up != nil {
				exp.sim.SetLink(body.From, body.To, *body.Up)
				if *body.Up {
					label = "link-up"
				} else {
					label = "link-down"
				}
			}
			if body.Delay != nil {
				if err := exp.sim.SetDelay(body.From, body.To, *body.Delay); err != nil {
					return "", err
				}
				label = "delay"
			}
			return label, nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/partition", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Groups [][]int `json:"groups"`
			}
			if err := decode(r, &body); err != nil || len(body.Groups) == 0 {
				return "", fmt.Errorf("groups required")
			}
			exp.sim.Partition(body.Groups)
			return "partition", nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/heal", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			exp.sim.HealAll()
			return "heal", nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/deliver-first", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Selector raft.Selector `json:"selector"`
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			if err := exp.sim.DeliverFirst(body.Selector); err != nil {
				return "", err
			}
			return "deliver-first", nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/postpone", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Selector raft.Selector `json:"selector"`
				Delay    int64         `json:"delay"`
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			if err := exp.sim.Postpone(body.Selector, body.Delay); err != nil {
				return "", err
			}
			return "postpone", nil
		})
	}))
	return mux
}

func main() {
	live := raft.Scenario{
		Name:          "live",
		Description:   "可交互新鲜五节点集群",
		Nodes:         []int{1, 2, 3, 4, 5},
		Seed:          1,
		InitialLeader: 1,
	}
	sim := raft.NewSimulator(live.Nodes, live.Seed)
	_ = sim.ElectInitial(live.InitialLeader)
	lab := &Lab{sim: sim, scenario: live}
	lab.loadExperiments()
	lab.loadAnalyses()
	log.Fatal(http.ListenAndServe(":8080", lab.routes()))
}
