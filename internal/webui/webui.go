// Package webui serves the embedded Orchestra console, a React single-page
// application compiled to static assets at build time. The console shell
// contains no cluster data; it renders a login gate and then calls the existing
// bearer-authenticated REST API to populate its views, so the master remains a
// single zero-runtime-dependency binary.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// static embeds the compiled console (index.html plus hashed assets) produced
// by `task web:build` into the binary.
//
//go:embed static
var static embed.FS

const (
	assetCacheControl = "public, max-age=31536000, immutable"
	indexCacheControl = "no-store"
	indexContentType  = "text/html; charset=utf-8"
)

// Handler returns an http.Handler that serves the console. It answers the root
// document and the hashed asset files; every other path 404s because the app
// uses hash-based client-side routing and never requests sub-paths.
func Handler() http.Handler {
	root, err := fs.Sub(static, "static")
	if err != nil {
		panic("webui: sub static directory: " + err.Error())
	}
	index, err := fs.ReadFile(root, "index.html")
	if err != nil {
		panic("webui: read embedded index.html: " + err.Error())
	}
	fileServer := http.FileServer(http.FS(root))

	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		path := strings.TrimPrefix(request.URL.Path, "/")
		switch {
		case path == "" || path == "index.html" || path == "console" || path == "ui":
			writer.Header().Set("Content-Type", indexContentType)
			writer.Header().Set("Cache-Control", indexCacheControl)
			writer.Header().Set("X-Content-Type-Options", "nosniff")
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write(index)
		case strings.HasPrefix(path, "assets/"):
			writer.Header().Set("Cache-Control", assetCacheControl)
			fileServer.ServeHTTP(writer, request)
		default:
			http.NotFound(writer, request)
		}
	})
}
