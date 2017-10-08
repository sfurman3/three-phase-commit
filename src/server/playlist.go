package main

import (
	"encoding/json"
	"io"
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
