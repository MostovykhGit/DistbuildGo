//go:build !solution

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

func NewBuildService(l *zap.Logger, s Service) *BuildHandler {
	return &BuildHandler{logger: l, service: s}
}

type BuildHandler struct {
	logger    *zap.Logger
	service   Service
	mutex     sync.Mutex
	isStarted bool
}

type BuildStatusWriter struct {
	w            *http.ResponseWriter
	rc           *http.ResponseController
	buildHandler *MainBuildHandler
}

func NewBuildStatusWriter(w *http.ResponseWriter, handler *MainBuildHandler) *BuildStatusWriter {
	var writer BuildStatusWriter
	writer.w = w
	writer.rc = http.NewResponseController(*w)
	writer.buildHandler = handler
	return &writer
}

func (bw *BuildStatusWriter) Started(rsp *BuildStarted) error {
	logger := bw.buildHandler.handler.logger
	defer bw.buildHandler.handler.mutex.Unlock()
	bw.buildHandler.handler.mutex.Lock()

	bw.buildHandler.handler.isStarted = true
	logger.Info("Started")
	var jsonEncoding []byte
	jsonEncoding, err := json.Marshal(rsp)
	if err != nil {
		logger.Error("marshal error in Started")
		return err
	}

	(*bw.w).Header().Set("Content-Type", "application/json")
	_, err = fmt.Fprintln(*bw.w, string(jsonEncoding))
	if err != nil {
		logger.Error("error after Wrtie in Started")
		return err
	}

	err = bw.rc.Flush()
	if err != nil {
		logger.Error("error after Flush in Started")
		return err
	}

	return nil
}

func (bw *BuildStatusWriter) Updated(update *StatusUpdate) error {
	logger := bw.buildHandler.handler.logger
	logger.Info("Updated")

	var jsonEncoding []byte
	jsonEncoding, err := json.Marshal(update)
	if err != nil {
		logger.Error("marshal error in Updated")
		return err
	}

	_, err = fmt.Fprintln(*bw.w, string(jsonEncoding))
	bw.buildHandler.handler.logger.Info("Updated: " + string(jsonEncoding))
	if err != nil {
		logger.Sugar().Infof("error after Write in Updated - %s", err.Error())
		return err
	}

	err = bw.rc.Flush()
	if err != nil {
		logger.Error("error after Flush in Updated")
		return err
	}

	return nil
}

type MainBuildHandler struct {
	handler *BuildHandler
}

func (buildHandler MainBuildHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler := buildHandler.handler
	handler.logger.Info("start ServeHTTP in build handler")

	defer r.Body.Close()
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		handler.logger.Error("failure after reading request body in build handler")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var buildRequest BuildRequest
	err = json.Unmarshal(reqBody, &buildRequest)
	if err != nil {
		handler.logger.Error("failure after unmarshaling request body in build handler")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	writer := NewBuildStatusWriter(&w, &buildHandler)
	ctx := r.Context()
	err = handler.service.StartBuild(ctx, &buildRequest, writer)
	defer handler.mutex.Unlock()
	handler.mutex.Lock()
	if err != nil {
		if handler.isStarted {
			buildHandler.handler.logger.Sugar().Infof("error while handler running: %s", err.Error())
			_ = writer.Updated(&StatusUpdate{BuildFailed: &BuildFailed{Error: err.Error()}})
		} else {
			handler.logger.Error("error in StartBuild of build service")
			handler.logger.Error(err.Error())

			fmt.Fprintln(w, err.Error())
			err := writer.rc.Flush()
			if err != nil {
				handler.logger.Error("error after flush after writing error in build handler")
			}
		}
		return
	}
	_ = writer.Updated(&StatusUpdate{BuildFailed: &BuildFailed{Error: io.EOF.Error()}})
}

type SignalHandler struct {
	handler *BuildHandler
}

func (signalHandler SignalHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler := signalHandler.handler
	defer r.Body.Close()

	if !r.URL.Query().Has("build_id") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	buildIDstr := r.URL.Query().Get("build_id")

	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var request SignalRequest
	err = json.Unmarshal(reqBody, &request)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	var buildID build.ID
	err = buildID.UnmarshalText([]byte(buildIDstr))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	resp, err := handler.service.SignalBuild(ctx, buildID, &request)
	if err != nil {
		handler.logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	respText, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(respText)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (h *BuildHandler) Register(mux *http.ServeMux) {
	mux.Handle("POST /signal", SignalHandler{handler: h})
	mux.Handle("POST /build", MainBuildHandler{handler: h})
}
