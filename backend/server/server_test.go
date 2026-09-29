package server

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

// chanListener feeds in-memory net.Pipe conns to a real http.Server,
// exercising the actual upgrade + WebSocket code without OS sockets.
type chanListener struct {
	ch chan net.Conn
}

func (l *chanListener) Accept() (net.Conn, error) {
	c, ok := <-l.ch
	if !ok {
		return nil, io.EOF
	}
	return c, nil
}
func (l *chanListener) Close() error   { close(l.ch); return nil }
func (l *chanListener) Addr() net.Addr { return dummyAddr{} }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "pipe" }
func (dummyAddr) String() string  { return "pipe" }

func startTestServer(t *testing.T, h *Hub) (net.Conn, func()) {
	t.Helper()
	ln := &chanListener{ch: make(chan net.Conn, 1)}
	mux := http.NewServeMux()
	hub := h
	hub.Routes(mux)
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)

	serverConn, clientConn := net.Pipe()
	ln.ch <- serverConn

	req := "GET /ws HTTP/1.1\r\nHost: lab\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := clientConn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := newBufioReader(clientConn)
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil || resp.StatusCode != 101 {
		t.Fatalf("handshake failed: %v %v", resp, err)
	}
	cleanup := func() {
		clientConn.Close()
		srv.Close()
	}
	return clientConn, cleanup
}

func readFrame(c net.Conn) ([]byte, error) {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(c, hdr); err != nil {
		return nil, err
	}
	opcode := hdr[0] & 0x0f
	if opcode == 0x8 {
		return nil, io.EOF
	}
	n := int(hdr[1] & 0x7f)
	if n == 126 {
		ext := make([]byte, 2)
		if _, err := io.ReadFull(c, ext); err != nil {
			return nil, err
		}
		n = int(ext[0])<<8 | int(ext[1])
	}
	data := make([]byte, n)
	_, err := io.ReadFull(c, data)
	return data, err
}

func masked(payload []byte) []byte {
	mask := [4]byte{1, 2, 3, 4}
	out := []byte{0x81}
	if len(payload) < 126 {
		out = append(out, 0x80|byte(len(payload)))
	} else {
		out = append(out, 0xfe, byte(len(payload)>>8), byte(len(payload)))
	}
	out = append(out, mask[:]...)
	for i, b := range payload {
		out = append(out, b^mask[i%4])
	}
	return out
}

type stateMsg struct {
	Snapshot json.RawMessage `json:"snapshot"`
}

func TestEndToEndElectionPartitionHeal(t *testing.T) {
	h := NewHub(t.TempDir())
	cli, cleanup := startTestServer(t, h)
	defer cleanup()

	readState := func() json.RawMessage {
		frame, err := readFrame(cli)
		if err != nil {
			t.Fatalf("frame: %v", err)
		}
		var st stateMsg
		json.Unmarshal(frame, &st)
		return st.Snapshot
	}
	readState() // initial snapshot

	send := func(cmd command) {
		b, _ := json.Marshal(cmd)
		if _, err := cli.Write(masked(b)); err != nil {
			t.Fatal(err)
		}
	}

	send(command{Type: "reset", Nodes: 5, Seed: 42})
	readState()
	send(command{Type: "step", Ticks: 60})
	var snap struct {
		Leader int `json:"leader"`
	}
	json.Unmarshal(readState(), &snap)
	if snap.Leader < 0 {
		t.Fatal("no leader elected over WebSocket")
	}
	leader := snap.Leader

	send(command{Type: "isolate", Node: leader})
	readState()
	send(command{Type: "propose", Node: leader, Data: "split-write"})
	readState()
	send(command{Type: "step", Ticks: 80})
	var nodes struct {
		Nodes []struct {
			ID        int `json:"id"`
			CommitIdx int `json:"commitIdx"`
			Log       struct {
				Entries []struct {
					Index int    `json:"index"`
					Data  string `json:"data"`
				} `json:"entries"`
			} `json:"log"`
		} `json:"nodes"`
	}
	json.Unmarshal(readState(), &nodes)
	for _, n := range nodes.Nodes {
		for _, e := range n.Log.Entries {
			if e.Data == "split-write" && e.Index <= n.CommitIdx {
				t.Fatalf("node %d committed a minority entry over WebSocket", n.ID)
			}
		}
	}

	send(command{Type: "heal", Node: leader})
	readState()
	send(command{Type: "step", Ticks: 80})
	var healed struct {
		Nodes []struct {
			CommitIdx int `json:"commitIdx"`
			Log       struct {
				Entries []struct {
					Index int    `json:"index"`
					Term  int    `json:"term"`
					Data  string `json:"data"`
				} `json:"entries"`
			} `json:"log"`
		} `json:"nodes"`
	}
	json.Unmarshal(readState(), &healed)
	minCommit := 1 << 30
	for _, n := range healed.Nodes {
		if n.CommitIdx < minCommit {
			minCommit = n.CommitIdx
		}
	}
	for idx := 1; idx <= minCommit; idx++ {
		ref := healed.Nodes[0].Log.Entries[idx-1]
		for _, n := range healed.Nodes[1:] {
			got := n.Log.Entries[idx-1]
			if got.Term != ref.Term || got.Data != ref.Data {
				t.Fatalf("committed prefix diverges at %d", idx)
			}
		}
	}
}

func TestSaveAndReplayScenario(t *testing.T) {
	h := NewHub(t.TempDir())
	run := func(cmd command) { h.handleCommand(nil, mustJSON(cmd)) }
	run(command{Type: "reset", Nodes: 5, Seed: 77})
	run(command{Type: "step", Ticks: 50})
	run(command{Type: "propose", Node: 0, Data: "rec"})
	run(command{Type: "step", Ticks: 20})
	run(command{Type: "isolate", Node: 0})
	run(command{Type: "step", Ticks: 30})
	run(command{Type: "heal", Node: 0})
	run(command{Type: "step", Ticks: 30})

	before := h.engine.Snapshot()
	s := h.engine.Scenario("ws-replay")

	h2 := NewHub(t.TempDir())
	run2 := func(cmd command) { h2.handleCommand(nil, mustJSON(cmd)) }
	run2(command{Type: "loadScenario"}) // nil scenario must be a safe no-op
	h2.loadScenarioLocked(s)
	for h2.engine.TickValue() < before.Tick {
		h2.engine.Tick()
	}
	after := h2.engine.Snapshot()
	for i := range before.Nodes {
		a, b := before.Nodes[i], after.Nodes[i]
		if a.Role != b.Role || a.Term != b.Term || a.CommitIdx != b.CommitIdx {
			t.Fatalf("replay mismatch node %d: %+v vs %+v", i, a, b)
		}
		if len(a.Log.Entries) != len(b.Log.Entries) {
			t.Fatalf("replay log length mismatch node %d", i)
		}
	}

	b := mustSave(t, h, "file-demo")
	if !strings.Contains(string(b), "file-demo") {
		t.Fatal("saved JSON missing scenario name")
	}
	if _, err := readFileOK(h.scenarioPath("file-demo")); err != nil {
		t.Fatalf("scenario file missing: %v", err)
	}
}
