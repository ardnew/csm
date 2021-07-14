package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

const (
	cacheFileName = ".suite.csm"
	cacheFileMode = 0644
)

type Cache struct {
	cacheFilePath string // path to the cache file
	archivePath   string // path (.zip) of the test case suite
	contentPath   string // path (directory) of uncompressed test case suite
	cached        *Suite // details stored in cache file
	actual        *Suite // details pulled from file system
}

type Suite struct {
	Name string `json:"name"`
	File []File `json:"file,omitempty"`
	Hash string `json:"hash,omitempty"`
}

type File struct {
	Name string `json:"name"`
	Size string `json:"size,omitempty"`
	Date string `json:"date,omitempty"`
	Hash string `json:"hash,omitempty"`
}

func (f File) Equals(g File) bool {
	return f.Name == g.Name && f.Size == g.Size && f.Hash == g.Hash
}

func New(archive, content string) *Cache {
	return &Cache{
		cacheFilePath: filepath.Join(content, cacheFileName),
		archivePath:   archive,
		contentPath:   content,
	}
}

func (c *Cache) Read() error {
	b, err := os.ReadFile(c.cacheFilePath)
	if nil != err {
		if os.IsNotExist(err) {
			return fmt.Errorf("will create new cache file (%w)", err)
		}
		return err
	}
	c.cached = &Suite{}
	return json.Unmarshal(b, c.cached)
}

func (c *Cache) Update() error {

	hash, err := hashFile(c.archivePath)
	if nil != err {
		return err
	}
	c.actual = &Suite{
		Name: filepath.Base(c.archivePath),
		File: []File{},
		Hash: hash,
	}

	filepath.Walk(c.contentPath,
		func(path string, info fs.FileInfo, err error) error {
			if nil != err {
				return fmt.Errorf("%w: %s", err, path)
			}
			if 0 == info.Mode()&fs.ModeType && info.Name() != cacheFileName {
				hash, err := hashFile(path)
				if nil != err {
					return err
				}
				c.actual.File = append(c.actual.File,
					File{
						Name: info.Name(),
						Size: strconv.FormatInt(info.Size(), 10),
						Date: info.ModTime().Local().String(),
						Hash: hash,
					})
			}
			return nil
		})

	return nil
}

func (c *Cache) Write() error {
	j, err := json.MarshalIndent(c.actual, "", "  ")
	if nil != err {
		return err
	}
	return os.WriteFile(c.cacheFilePath, j, cacheFileMode)
}

func (c *Cache) Stale() (bool, []string) {

	if c.cached == nil || c.actual == nil ||
		c.cached.Hash == "" || c.actual.Hash == "" {
		return true, nil
	}

	if c.cached.Hash != c.actual.Hash {
		stale := []string{}
		cache := map[string]File{}
		for _, f := range c.cached.File {
			cache[f.Name] = f
		}
		for _, f := range c.actual.File {
			prev, seen := cache[f.Name]
			if !seen || !f.Equals(prev) {
				stale = append(stale, f.Name)
			}
		}
		return true, stale
	}
	return false, nil
}

func hashFile(path string) (string, error) {

	f, err := os.Open(path)
	if nil != err {
		return "", err
	}
	defer f.Close()

	h := xxhash.New()
	_, err = io.Copy(h, f)
	if nil != err {
		return "", err
	}

	return strconv.FormatUint(h.Sum64(), 16), nil
}
