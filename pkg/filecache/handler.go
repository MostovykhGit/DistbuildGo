//go:build !solution

package filecache

import (
	"io"
	"net/http"
	"os"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

type Handler struct {
	logger      *zap.Logger
	remoteCache *Cache
	group       *singleflight.Group
	results     map[string]error
}

type DownloadHandler struct {
	handler *Handler
}

func (h *DownloadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !r.URL.Query().Has("id") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	artifactStr := r.URL.Query().Get("id")
	var fileID build.ID
	err := fileID.UnmarshalText([]byte(artifactStr))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	path, unlock, err := h.handler.remoteCache.Get(fileID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	content, err := os.ReadFile(path)
	unlock()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(content)
}

type UploadHandler struct {
	handler *Handler
}

func (h *UploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !r.URL.Query().Has("id") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	artifactStr := r.URL.Query().Get("id")
	var fileID build.ID
	err := fileID.UnmarshalText([]byte(artifactStr))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	defer r.Body.Close()
	content, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err, _ = h.handler.group.Do(artifactStr, func() (interface{}, error) {
		if val, ok := h.handler.results[artifactStr]; ok {
			return nil, val
		}

		var resultError error
		defer func() {
			h.handler.results[artifactStr] = resultError
		}()

		uploadWriter, abort, resultError := h.handler.remoteCache.Write(fileID)
		if err != nil {
			return nil, resultError
		}

		_, resultError = uploadWriter.Write(content)
		if resultError != nil {
			_ = abort()
			return nil, resultError
		}

		uploadWriter.Close()
		return nil, nil
	})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func NewHandler(l *zap.Logger, cache *Cache) *Handler {
	var handler Handler
	handler.logger = l
	handler.remoteCache = cache
	handler.group = new(singleflight.Group)
	handler.results = make(map[string]error)
	return &handler
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("PUT /file", &UploadHandler{h})
	mux.Handle("GET /file", &DownloadHandler{h})
}
