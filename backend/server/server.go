package server

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"raftlab/raft"
)

type Hub struct {
	mu      sync.Mutex
	engine  *raft.Engine
	clients map[*wsConn]bool
	running bool
	tickMS  int
	speed   int
	dir     string

	stopCh chan struct{}
}

func NewHub(dir string) *Hub {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("scenario dir: %v", err)
	}
	h := &Hub{
		engine:  raft.NewEngine(5, int(time.Now().UnixNano())),
		clients: map[*wsConn]bool{},
		tickMS:  60,
		speed:   1,
		dir:     dir,
	}
	return h
}

func (h *Hub) broadcast(v any) {
	b, _ := json.Marshal(v)
	for c := range h.clients {
		if err := c.writeText(b); err != nil {
			delete(h.clients, c)
			c.close()
		}
	}
}

// stateMsg pushes the snapshot and the complete event timeline.
func (h *Hub) stateMsg() map[string]any {
	h.mu.Lock()
	allEvents := h.engine.Events()
	msg := map[string]any{
		"type":     "state",
		"snapshot": h.engine.Snapshot(),
		"events":   allEvents,
		"running":  h.running,
		"tickMS":   h.tickMS,
		"speed":    h.speed,
		"scenario": h.engine.Actions(),
	}
	h.mu.Unlock()
	return msg
}

func (h *Hub) pushState() { h.broadcast(h.stateMsg()) }

// Run is the wall-clock driver: every tickMS it advances the logical
// clock one tick while running. Pausing freezes time entirely.
func (h *Hub) Run() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var last time.Time
	for range ticker.C {
		h.mu.Lock()
		running := h.running
		interval := time.Duration(h.tickMS/h.effectiveSpeed()) * time.Millisecond
		h.mu.Unlock()
		if !running {
			last = time.Time{}
			continue
		}
		now := time.Now()
		if last.IsZero() {
			last = now
		}
		if now.Sub(last) < interval {
			continue
		}
		last = now
		h.mu.Lock()
		h.engine.Tick()
		h.mu.Unlock()
		h.pushState()
	}
}

func (h *Hub) effectiveSpeed() int {
	if h.speed < 1 {
		return 1
	}
	if h.speed > 16 {
		return 16
	}
	return h.speed
}

type command struct {
	Type     string         `json:"type"`
	Node     int            `json:"node"`
	NodeB    int            `json:"nodeB"`
	Data     string         `json:"data"`
	DelayMin int            `json:"delayMin"`
	DelayMax int            `json:"delayMax"`
	LossPct  int            `json:"lossPct"`
	Ticks    int            `json:"ticks"`
	TickMS   int            `json:"tickMS"`
	Speed    int            `json:"speed"`
	Nodes    int            `json:"nodes"`
	Seed     int            `json:"seed"`
	Name     string         `json:"name"`
	Scenario *raft.Scenario `json:"scenario"`
}

func (h *Hub) handleCommand(c *wsConn, raw []byte) {
	var cmd command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return
	}
	h.mu.Lock()
	switch cmd.Type {
	case "play":
		h.running = true
	case "pause":
		h.running = false
	case "step":
		h.running = false
		n := cmd.Ticks
		if n < 1 {
			n = 1
		}
		for i := 0; i < n; i++ {
			h.engine.Tick()
		}
	case "propose":
		h.engine.Propose(cmd.Node, cmd.Data)
	case "isolate":
		h.engine.Isolate(cmd.Node)
	case "heal":
		h.engine.Heal(cmd.Node)
	case "setLink":
		h.engine.SetLink(cmd.Node, cmd.NodeB, cmd.DelayMin, cmd.DelayMax, cmd.LossPct)
	case "setTiming":
		if cmd.TickMS >= 10 {
			h.tickMS = cmd.TickMS
		}
		if cmd.Speed >= 1 {
			h.speed = cmd.Speed
		}
	case "reset":
		nodes := cmd.Nodes
		if nodes < 1 || nodes > 9 {
			nodes = 5
		}
		seed := cmd.Seed
		if seed == 0 {
			seed = int(time.Now().UnixNano())
		}
		h.engine = raft.NewEngine(nodes, seed)
		h.running = false
	case "loadScenario":
		if cmd.Scenario != nil {
			h.loadScenarioLocked(*cmd.Scenario)
		}
	}
	h.mu.Unlock()
	h.pushState()
}

func (h *Hub) loadScenarioLocked(s raft.Scenario) {
	nodes := s.Nodes
	if nodes < 1 {
		nodes = 5
	}
	h.engine = raft.ReplayEngine(raft.Scenario{
		Name:    s.Name,
		Nodes:   nodes,
		Seed:    s.Seed,
		Actions: s.Actions,
	})
	h.running = false
}

func (h *Hub) serveWS(w http.ResponseWriter, r *http.Request) {
	c, err := upgrade(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	h.clients[c] = true
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.clients, c)
		h.mu.Unlock()
		c.close()
	}()
	_ = c.writeText(mustJSON(h.stateMsg()))
	for {
		raw, err := c.readMessage()
		if err != nil {
			return
		}
		h.handleCommand(c, raw)
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// -------- scenario save/list/load (REST) --------

func (h *Hub) scenarioPath(name string) string {
	return filepath.Join(h.dir, sanitize(name)+".json")
}

func sanitize(name string) string {
	if name == "" {
		name = "scenario"
	}
	out := make([]rune, 0, len(name))
	for _, ch := range name {
		if ch == '/' || ch == '\\' || ch == '.' {
			ch = '_'
		}
		out = append(out, ch)
	}
	return string(out)
}

func (h *Hub) handleSave(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	h.mu.Lock()
	s := h.engine.Scenario(req.Name)
	h.mu.Unlock()
	b, _ := json.MarshalIndent(s, "", "  ")
	if err := os.WriteFile(h.scenarioPath(req.Name), b, 0o644); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Write(b)
}

func (h *Hub) handleList(w http.ResponseWriter, r *http.Request) {
	entries, _ := os.ReadDir(h.dir)
	names := []string{}
	for _, en := range entries {
		if filepath.Ext(en.Name()) == ".json" {
			names = append(names, en.Name()[:len(en.Name())-5])
		}
	}
	sort.Strings(names)
	json.NewEncoder(w).Encode(names)
}

func (h *Hub) handleLoad(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	b, err := os.ReadFile(h.scenarioPath(name))
	if err != nil {
		http.Error(w, "not found", 404)
		return
	}
	var s raft.Scenario
	if err := json.Unmarshal(b, &s); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	h.mu.Lock()
	h.loadScenarioLocked(s)
	h.mu.Unlock()
	h.pushState()
	json.NewEncoder(w).Encode(s)
}

func (h *Hub) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/ws", h.serveWS)
	mux.HandleFunc("/api/scenarios", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			h.handleList(w, r)
		case http.MethodPost:
			h.handleSave(w, r)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	mux.HandleFunc("/api/scenario", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			h.handleLoad(w, r)
			return
		}
		http.Error(w, "method not allowed", 405)
	})
}
