package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strings"

	"raftlab/server"
)

//go:embed all:webdist
var distFS embed.FS

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	scenarios := flag.String("scenarios", "scenarios", "scenario storage directory")
	flag.Parse()

	hub := server.NewHub(*scenarios)
	go hub.Run()

	mux := http.NewServeMux()
	hub.Routes(mux)

	static, err := fs.Sub(distFS, "webdist")
	if err != nil {
		log.Fatal(err)
	}
	fileServer := http.FileServer(http.FS(static))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/ws" {
			http.NotFound(w, r)
			return
		}
		// SPA fallback to index.html for client-side routes.
		if _, err := fs.Stat(static, strings.TrimPrefix(r.URL.Path, "/")); err != nil {
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})

	fmt.Printf("Raft lab on http://localhost%s\n", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
