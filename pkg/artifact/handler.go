//go:build !solution

package artifact

import (
	"net/http"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/tarstream"
	"go.uber.org/zap"
)

type Handler struct {
	logger      *zap.Logger
	remoteCache *Cache
}

func NewHandler(l *zap.Logger, c *Cache) *Handler {
	var handler Handler
	handler.logger = l
	handler.remoteCache = c
	return &handler
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !r.URL.Query().Has("id") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	artifactStr := r.URL.Query().Get("id")
	var artifact build.ID
	err := artifact.UnmarshalText([]byte(artifactStr))
	if err != nil {
		h.logger.Error("wrong artifact id in artifact handler")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	dir, unlock, err := h.remoteCache.Get(artifact)
	if err != nil {
		h.logger.Error("Get from remote cache returned error")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = tarstream.Send(dir, w)
	unlock()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("GET /artifact", h)
}
