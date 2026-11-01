package webui

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandler_ServesConsoleAtRoot(t *testing.T) {
	handler := Handler()

	for _, path := range []string{"/", "/index.html", "/console", "/ui"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://orchestra.test"+path, nil)
		handler.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusOK, recorder.Code, "expected 200 for %s", path)
		require.Equal(t, "text/html; charset=utf-8", recorder.Header().Get("Content-Type"))
		require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
		require.Contains(t, recorder.Body.String(), "<div id=\"root\"></div>")
		require.Contains(t, recorder.Body.String(), "Orchestra")
	}
}

func TestHandler_ServesEmbeddedAssets(t *testing.T) {
	handler := Handler()

	root, err := fs.Sub(static, "static")
	require.NoError(t, err)
	assetPaths, err := fs.Glob(root, "assets/*")
	require.NoError(t, err)
	require.NotEmpty(t, assetPaths, "expected compiled assets to be embedded")

	for _, assetPath := range assetPaths {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://orchestra.test/"+assetPath, nil)
		handler.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusOK, recorder.Code, "expected 200 for /%s", assetPath)
		require.Equal(t, assetCacheControl, recorder.Header().Get("Cache-Control"))
	}
}

func TestHandler_ReturnsNotFoundForUnknownPaths(t *testing.T) {
	handler := Handler()

	for _, path := range []string{"/api/status", "/favicon.ico", "/mcp", "/assets"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://orchestra.test"+path, nil)
		handler.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusNotFound, recorder.Code, "expected 404 for %s", path)
	}
}
