package main

import (
	"encoding/json"
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
	pFile := ""
	backup := PLAYLIST + ".backup"
	if _, err := os.Stat(backup); !os.IsNotExist(err) {
		Error("server failed to finish last update to %s", PLAYLIST)
		os.Remove(PLAYLIST)
		os.Rename(backup, PLAYLIST)
		pFile = backup
	} else {
		if _, err := os.Stat(PLAYLIST); !os.IsNotExist(err) {
			pFile = PLAYLIST
		}
	}

	if pFile == "" {
		return NewPlaylist(), nil
	}

	f, err := os.Open(pFile)
	if err != nil {
		return p, err
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return p, err
	}

	err = json.Unmarshal(b, &p.value)
	return p, err
}

func (p *playlist) GetSong(song string) string {
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

func (p *playlist) DeleteSong(song, url string) {
	p.mutex.Lock()
	delete(p.value, song)
	p.mutex.Unlock()
}

func (p *playlist) WritePlaylist() {
	p.mutex.Lock()
	playlistJson, _ := json.Marshal(p.value)
	p.mutex.Unlock()

	backup := PLAYLIST + ".backup"
	os.Rename(PLAYLIST, backup)

	file, _ := os.Create(PLAYLIST)
	defer file.Close()

	file.Write(playlistJson)
	os.Remove(backup)
}
