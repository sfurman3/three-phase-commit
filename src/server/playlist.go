package main

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type playlist struct {
	value map[string]string
	mutex sync.Mutex
}

func NewPlaylist() playlist {
	return playlist{
		value: make(map[string]string),
	}
}

func ReadPlaylist() (playlist, error) {
	var p playlist
	if _, err := os.Stat(DT_LOG); os.IsNotExist(err) {
		return playlist{}, errors.New("no backup")
	}

	f, err := os.Open(DT_LOG)
	if err != nil {
		return p, err
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return p, err
	}

	// TODO: Read each entry from the DT log

	err = json.Unmarshal(b, &p.value)
	return p, err
}

func (p *playlist) GetSongUrl(song string) string {
	p.mutex.Lock()
	url, ok := p.value[song]
	p.mutex.Unlock()

	if ok {
		return url
	}

	return "NONE"
}

func (p *playlist) AddOrUpdateSong(song, url string) {
	p.mutex.Lock()
	p.value[song] = url
	p.mutex.Unlock()
	// TODO: write to DT log?
}

func (p *playlist) DeleteSong(song string) {
	p.mutex.Lock()
	delete(p.value, song)
	p.mutex.Unlock()
}

func (p *playlist) WritePlaylist(w io.Writer) {
	p.mutex.Lock()
	playlistJson, _ := json.Marshal(p.value)
	p.mutex.Unlock()

	w.Write(playlistJson)
}
