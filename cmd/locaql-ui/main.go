package main

import (
	"embed"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

//go:embed web/*
var webFS embed.FS

func main() {
	fs := flag.NewFlagSet("locaql-ui", flag.ContinueOnError)
	addr := fs.String("addr", ":9070", "http address for the UI service")
	emulatorBaseURL := fs.String("emulator", "http://localhost:9050", "base URL of the LocaQL emulator")
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}

	h, err := newHandler(*emulatorBaseURL)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("LocaQL UI listening on %s (emulator: %s)", *addr, *emulatorBaseURL)
	if err := http.ListenAndServe(*addr, h); err != nil {
		log.Fatal(err)
	}
}

func newHandler(emulatorBaseURL string) (http.Handler, error) {
	target, err := url.Parse(strings.TrimSpace(emulatorBaseURL))
	if err != nil {
		return nil, err
	}
	if target.Scheme == "" || target.Host == "" {
		return nil, &url.Error{Op: "parse", URL: emulatorBaseURL, Err: errInvalidEmulatorURL{}}
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		trimmed := strings.TrimPrefix(req.URL.Path, "/api")
		if trimmed == "" {
			trimmed = "/"
		}
		req.URL.Path = singleJoiningSlash(target.Path, trimmed)
		req.Host = target.Host
	}

	staticFiles := http.FileServer(http.FS(webFS))
	mux := http.NewServeMux()
	mux.HandleFunc("/config", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"emulator": target.String()})
	})
	mux.Handle("/api/", proxy)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.ServeFileFS(w, r, webFS, "web/index.html")
			return
		}
		staticFiles.ServeHTTP(w, r)
	})
	return mux, nil
}

type errInvalidEmulatorURL struct{}

func (errInvalidEmulatorURL) Error() string {
	return "emulator URL must include scheme and host"
}

func singleJoiningSlash(a, b string) string {
	aSlash := strings.HasSuffix(a, "/")
	bSlash := strings.HasPrefix(b, "/")
	switch {
	case aSlash && bSlash:
		return a + b[1:]
	case !aSlash && !bSlash:
		return a + "/" + b
	default:
		return a + b
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
