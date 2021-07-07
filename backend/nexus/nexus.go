package nexus

import (
	"context"
	"fmt"
	"path/filepath"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/cache"
	"github.com/rclone/rclone/lib/bucket"
	"github.com/rclone/rclone/lib/rest"
)


// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "nexus",
		Description: "Nexus Repository Manager",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "username",
			Help:     "Account username",
			Required: true,
		}, {
			Name:     "password",
			Help:     "Application password",
			Required: true,
		}, {
			Name:     "endpoint",
			Help:     "Endpoint for the service.",
			Advanced: true,
		}},
	})
}

type Options struct {
	Username        string               `config:"username"`
	Password        string               `config:"password"`
	Endpoint        string               `config:"endpoint"`
}

type Fs struct {
	name            string
	root            string
	repository      string
	path            string
	opt             Options
	client          *rest.Client
	features        *fs.Features
	cache           *cache.Cache
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	f := &Fs{
		name:       name,
		client:     rest.NewClient(fshttp.NewClient(ctx)).SetErrorHandler(errorHandler),
		opt:        *opt,
		cache:      cache.New(),
	}
	f.setLocation(root)
	f.features = (&fs.Features{
		ReadMimeType:      true,
		WriteMimeType:     true,
		BucketBased:       true,
		BucketBasedRootOK: true,
	}).Fill(ctx, f)
	fs.Debugf(f, "Creating FS %s, repository %s", f.Name(), f.Repository())
	fs.Debugf(f, "Using username %s, URL %s", f.opt.Username, f.opt.Endpoint)
	return f, nil
}

func (f *Fs) Name() string {
	return f.name
}

func (f *Fs) Root() string {
	return f.root
}

func (f *Fs) Repository() string {
	return f.repository
}

func (f *Fs) Path() string {
	return f.path
}

func (f *Fs) String() string {
	return "nexus"
}

func (f *Fs) Precision() time.Duration {
	return time.Millisecond
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA1)
}

func (f *Fs) split(rootRelativePath string) (repository, absolutePath string) {
	return bucket.Split(path.Join(f.root, rootRelativePath))
}

type ListCallbackFunc func(item ListAssetsItemResponse) error

func (f *Fs) QueryAssets(ctx context.Context, repository string, callback ListCallbackFunc) error {
	opts := rest.Opts{
		Method:            "GET",
		Path:              "service/rest/v1/assets",
		RootURL:           f.opt.Endpoint,
		UserName:          f.opt.Username,
		Password:          f.opt.Password,
		Parameters:        url.Values{},
	}
	opts.Parameters.Set("repository", repository)
	items := new(ListAssetsResponse)
	done := false
	for ! done {
		items = new(ListAssetsResponse)
		fs.Debugf(f, "Query")
		// perform query
		f.client.CallJSON(ctx, &opts, nil, &items)
		for _, item := range items.Items {
			callback(item)
		}
		if items.ContinuationToken == nil {
			break
		}
		opts.Parameters.Set("continuationToken", *items.ContinuationToken)
		fs.Debugf(f, "Continuing with %s", *items.ContinuationToken)
	}
	return nil
}

func Parent(fullpath string) (parent string, basename string) {
	parent, basename = path.Split(fullpath)
	parent = strings.Trim(parent, "/")
	return
}

func (f *Fs) ConditionnalyUpdateParent(cacheEntry CacheItem, childpath string) error {
	parentpath, _ := Parent(childpath)
	childFound := false
	for _, child := range cacheEntry.children {
		if child == childpath {
			childFound = true
		}
	}
	if ! childFound {
		cacheEntry.children = append(cacheEntry.children, childpath)
		f.CachePut(parentpath, cacheEntry)
	}
	return nil
}

func (f *Fs) GetOrCacheDir(repository string, dirpath string, childpath string) (cacheEntry CacheItem) {
	cacheKey := path.Join(repository, dirpath)
	item, found := f.cache.GetMaybe(cacheKey)
	if found {
		cacheEntry = item.(CacheItem)
		if childpath != repository {
			f.ConditionnalyUpdateParent(cacheEntry, childpath)
		}
	} else {
		dirEntry := fs.NewDir(cacheKey, time.Now())
		cacheEntry = CacheItem{}
		cacheEntry.directory = dirEntry
		if childpath != repository {
			cacheEntry.children = append(cacheEntry.children, childpath)
		}
		f.CachePut(dirEntry.Remote(), cacheEntry)
	}
	return cacheEntry
}

func (f *Fs) ProcessAssets(ctx context.Context, repository string) error {
	return f.QueryAssets(ctx, repository, func (item ListAssetsItemResponse) error {
		// all items are files
		// dirs must be rebuilt and stored from file path

		// set root entry
		rootEntry := f.GetOrCacheDir(repository, "", repository)

		// first process parent dirs
		filedir, _ := Parent(item.Path)
		dir := filedir
		// last is the last processed dir
		// used to get child each time we navigate to the root
		last := path.Join(repository, item.Path)

		// keep file directory
		var filedirCacheEntry *CacheItem

		// recurse all parents dirs until root
		for dir != "" {
			// cache dir, append child
			dirCacheEntry := f.GetOrCacheDir(repository, dir, last)
			// update last processed dir
			last = path.Join(repository, dir)
			// move to parent
			dir, _ = Parent(dir)
			// store filedir CacheItem
			if filedirCacheEntry == nil {
				filedirCacheEntry = &dirCacheEntry
			}
			if dir == "" {
				f.GetOrCacheDir(repository, dir, last)
			}
		}
		// if filedirCacheEntry not set, then root is the file parent
		if filedirCacheEntry == nil {
			filedirCacheEntry = &rootEntry
		}
		
		// process file
		o := &Object{
			remote: path.Join(repository, item.Path),
			modTime: time.Now(),
		}
		cacheEntry := CacheItem{}
		cacheEntry.file = o
		f.CachePut(o.Remote(), cacheEntry)
		f.ConditionnalyUpdateParent(*filedirCacheEntry, o.Remote())

		return nil
	})
}

func (f *Fs) CachePut(path string, item CacheItem) {
	fs.Debugf(f, "Caching %s", path)
	f.cache.Put(path, item)
}

func (f *Fs) Recurse(base string, cacheKey string, recurse bool) (entries fs.DirEntries) {
	entry, found := f.cache.GetMaybe(cacheKey)
	if ! found {
		fs.Debugf(f, "Not found %s", cacheKey)
		return entries
	}
	item := entry.(CacheItem)
	if ! recurse {
		relpath, _ := filepath.Rel(base, cacheKey)
		if item.directory != nil {
			directory := fs.NewDir(relpath, time.Now())
			entries = append(entries, directory)
		} else if item.file != nil {
			file := item.file
			file.remote = relpath
			entries = append(entries, file)
		}
	} else {
		fs.Debugf(f, "recurse %s %s", cacheKey, item.children)
		for _, child := range item.children {
			recursed := f.Recurse(base, child, false)
			for _, rec := range recursed {
				entries = append(entries, rec)
			}
		}
	}
	fs.Debugf(f, "recurse %s", entries)
	return entries
}

func (f *Fs) List(ctx context.Context, relativeDir string) (entries fs.DirEntries, err error) {
	repository, directory := f.split(relativeDir)
	_, found := f.cache.GetMaybe(repository)
	if ! found {
		f.ProcessAssets(ctx, repository)
	}
	entries = f.Recurse(f.Root(), path.Join(repository, directory), true)
	fs.Debugf(f, "recurse %s", entries)
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return nil, nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, nil
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return nil
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return nil
}

// parsePath parses a remote 'url'
func parsePath(path string) (root string) {
	root = strings.Trim(path, "/")
	return
}

// setRoot changes the root of the Fs
func (f *Fs) setLocation(root string) {
	f.root = parsePath(root)
	f.repository, f.path = bucket.Split(f.root)
}

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	// Decode error response
	errResponse := new(Error)
	err := rest.DecodeJSON(resp, &errResponse)
	if err != nil {
		fs.Debugf(nil, "Couldn't decode error response: %v", err)
	}
	if errResponse.Code == "" {
		errResponse.Code = "unknown"
	}
	if errResponse.Status == 0 {
		errResponse.Status = resp.StatusCode
	}
	if errResponse.Message == "" {
		errResponse.Message = "Unknown " + resp.Status
	}
	return errResponse
}

type Error struct {
	Status  int    `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error satisfies the error interface
func (e *Error) Error() string {
	return fmt.Sprintf("%s (%d %s)", e.Message, e.Status, e.Code)
}

// Fatal satisfies the Fatal interface
//
// It indicates which errors should be treated as fatal
func (e *Error) Fatal() bool {
	return e.Status == 403 // 403 errors shouldn't be retried
}

type Checksum struct {
	Sha1               string             `json:"sha1"`
	Md5                string             `json:"md5"`
}

type AssetResponse struct {
	BlobCreated        time.Time          `json:"blobCreated"`
	LastDownloaded     time.Time          `json:"lastDownloaded"`
	LastModified       time.Time          `json:"lastModified"`
	ContentType        string             `json:"contentType"`

	Id                 string             `json:"id"`
	Checksum           Checksum           `json:"checksum"`
	DownloadUrl        string             `json:"downloadUrl"`
	Path               string             `json:"path"`
	Repository         string             `json:"repository"`
	Format             string             `json:"format"`
}

type ListAssetsItemResponse struct {
	Id                 string             `json:"id"`
	DownloadUrl        string             `json:"downloadUrl"`
	Path               string             `json:"path"`
	Repository         string             `json:"repository"`
	Format             string             `json:"format"`
	Checksum           Checksum           `json:"checksum"`
}

type ListAssetsResponse struct {
	Items              []ListAssetsItemResponse    `json:"items"`
	ContinuationToken  *string                      `json:"continuationToken"`
}

type Object struct {
	fs                 *Fs
	checksum           Checksum
	remote             string
	modTime            time.Time
	size               int64
}

type CacheItem struct {
	directory          *fs.Dir
	file               *Object
	children           []string
}

func (object* Object) String() string {
	return object.remote
}

func (object* Object) Remote() string {
	return object.remote
}

func (object* Object) ModTime(context.Context) time.Time {
	return object.modTime
}

func (object* Object) Size() int64 {
	return object.size
}

func (object* Object) Fs() fs.Info {
	return object.fs
}

func (object* Object) SetModTime(ctx context.Context, t time.Time) error {
	object.modTime = t
	return nil
}

func (object* Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return object.checksum.Md5, nil
}

func (object* Object) Storable() bool {
	return true
}

func (object* Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	return nil, nil
}

func (object* Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return nil
}

func (object* Object) Remove(ctx context.Context) error {
	return nil
}