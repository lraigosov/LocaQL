package server

import (
	_ "embed"
	"net/http"
	"strings"
)

//go:embed assets/bigquery_discovery_v2.json
var bigQueryDiscoveryTemplate string

// bigQueryDiscoveryDocument serves LocaQL's own, deliberately scoped
// BigQuery discovery document at /$discovery/rest?version=v2 — required for
// the official `bq` CLI to work at all: unlike every other official client
// library (Python, Node.js, Go, Java), `bq` is built on the older
// apitools/Discovery-API-driven generator and fetches this document before
// issuing any real API call. rootUrl/mtlsRootUrl/baseUrl in the embedded
// template point at a placeholder, rewritten here to the request's own
// Host so `bq`'s subsequent calls come back to this same server rather
// than real Google infrastructure. See KNOWN-DIVERGENCES.md for the
// intentional scope (jobs.* only, not the full real BigQuery API surface)
// and internal/server/assets/bigquery_discovery_v2.json for the document
// itself, hand-written to describe only what LocaQL actually implements.
func (s *Server) bigQueryDiscoveryDocument(w http.ResponseWriter, r *http.Request) {
	rootURL := "http://" + r.Host
	doc := strings.ReplaceAll(bigQueryDiscoveryTemplate, "__ROOT_URL__", rootURL)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(doc))
}
