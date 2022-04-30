package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	// "strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type httpService struct {
	addr   string
	store  *server
	logger *zap.Logger
}

func (s *httpService) handleKeyGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	// key, err := strconv.ParseUint(vars["id"], 10, 64)
	key := vars["id"]
	relation := vars["relation"]
	// if err != nil {
	// 	http.Error(w, "Wrong key", 400)
	// 	return
	// }
	value, err := s.store.get(key, relation)
	if err != nil {
		http.Error(w, "Could not get the key", 500)
		return
	}
	// valueS := strconv.FormatUint(value, 10)
	// valueS := [1]string{value}
	valueM, _ := json.Marshal(value)
	_, err = w.Write(valueM)
	// _, err = w.Write([]byte(valueS))
	if err != nil {
		http.Error(w, "Error in writing response", 500)
		return
	}
}

func (s *httpService) handleKeyPut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	// key, err := strconv.ParseUint(vars["id"], 10, 64)
	key := vars["id"]
	relation := vars["relation"]
	// if err != nil {
	// 	http.Error(w, "Wrong key", 400)
	// 	return
	// }
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "Could not open request body", 500)
		return
	}
	type message struct {
		Value string `json:"value"`
	}
	var msg message
	err = json.Unmarshal(b, &msg)
	if err != nil {
		http.Error(w, "Could not parse Request body", 400)
		return
	}
	value := msg.Value
	err = s.store.put(key, relation, value)
	if err != nil {
		http.Error(w, "Could not put the key", 500)
		return
	}
	// valueS := strconv.FormatUint(value, 10)
	valueS := value
	_, err = w.Write([]byte(valueS))
	if err != nil {
		http.Error(w, "Error in writing response", 500)
		return
	}
}

func (s *httpService) handleKeyDelete(w http.ResponseWriter, r *http.Request) {
	// TODO
	vars := mux.Vars(r)
	// key, err := strconv.ParseUint(vars["id"], 10, 64)
	key := vars["id"]
	// if err != nil {
	// 	http.Error(w, "Wrong key", 400)
	// 	return
	// }
	err := s.store.delete(key)
	if err != nil {
		http.Error(w, "Could not delete the key", 500)
		return
	}
	_, err = w.Write([]byte("0"))
	if err != nil {
		http.Error(w, "Error in writing response", 500)
		return
	}
}

func (s *httpService) handleJoin(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Got join message")
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "Could not open request body", 500)
		return
	}
	type message struct {
		Addr string `json:"Addr"`
		Id   string `json:"id"`
	}
	var msg message
	err = json.Unmarshal(b, &msg)
	if err != nil {
		s.logger.Info("Unable to unmarshal json")
	}
	fmt.Println(string(b))
	err = s.store.join(msg.Addr, msg.Id)
	if err != nil {
		s.logger.Info("The node could not join")
		http.Error(w, "The requesting node could not join", 500)
		return
	}
}

func (s *httpService) Start() {
	s.logger.Info("Server Starting", zap.String("address", s.addr))
	r := mux.NewRouter()
	r.HandleFunc("/join", s.handleJoin).Methods("POST")
	r.HandleFunc("/{id}/{relation}", s.handleKeyGet).Methods("GET")
	r.HandleFunc("/{id}/{relation}", s.handleKeyPut).Methods("PUT")
	r.HandleFunc("/{id}/{relation}", s.handleKeyDelete).Methods("DELETE")
	http.Handle("/", r)
	srv := http.Server{
		Handler: r,
		Addr:    s.addr,
	}
	log.Fatal(srv.ListenAndServe())
}
