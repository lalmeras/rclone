package nexus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/pcloud/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/bucket"
	"github.com/rclone/rclone/lib/errors"
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
			Help:     "Nexus username",
			Required: true,
		}, {
			Name:     "password",
			Help:     "Nexus password",
			Required: true,
		}, {
			Name:     "endpoint",
			Help:     "Endpoint for the service (https://nexus.host).",
			Advanced: true,
		}},
	})
}

type Options struct {
	Username string `config:"username"`
	Password string `config:"password"`
	Endpoint string `config:"endpoint"`
}

type Fs struct {
	name       string
	root       string
	repository string
	path       string
	opt        Options
	client     *rest.Client
	features   *fs.Features
	dirtree    *dirtree.DirTree
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	f := &Fs{
		name:    name,
		client:  rest.NewClient(fshttp.NewClient(ctx)).SetErrorHandler(errorHandler),
		opt:     *opt,
		dirtree: nil,
	}
	f.setLocation(root)
	f.features = (&fs.Features{
		ReadMimeType:            true,
		WriteMimeType:           true,
		BucketBased:             true,
		BucketBasedRootOK:       true,
		SlowModTime:             true,
		CanHaveEmptyDirectories: false,
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
	return hash.NewHashSet(hash.SHA1, hash.MD5)
}

func (f *Fs) split(rootRelativePath string) (repository, absolutePath string) {
	return bucket.Split(path.Join(f.root, rootRelativePath))
}

// list a whole repository ; fs.ListRCallback consumes results and handles directory extrapolation
// (directory are not listed)
func (f *Fs) listR(ctx context.Context, repository string, callback fs.ListRCallback) error {
	// common parameters
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/service/rest/v1/assets",
		RootURL:    f.opt.Endpoint,
		UserName:   f.opt.Username,
		Password:   f.opt.Password,
		Parameters: url.Values{},
	}
	opts.Parameters.Set("repository", repository)
	var items *ListAssetsResponse
	var entries fs.DirEntries
	done := false
	for !done {
		// perform until all results are retrieved
		items = new(ListAssetsResponse)
		// rest call
		f.client.CallJSON(ctx, &opts, nil, &items)
		for _, item := range items.Items {
			// entries to objects (all results are file)
			entries = append(entries, f.itemToObject(ctx, repository, item))
		}
		// exit if no more page
		if items.ContinuationToken == nil {
			break
		}
		// update page parameter
		opts.Parameters.Set("continuationToken", *items.ContinuationToken)
		fs.Debugf(f, "Continuing with %s", *items.ContinuationToken)
	}
	callback(entries)
	return nil
}

// transform REST results to Object (REST API only returns files)
// Two additional calls are needed :
// - GET /service/rest/v1/assets/{assetId} : modTime, checksums
// - HEAD {asset.DownloadUrl} : size
func (f *Fs) itemToObject(ctx context.Context, repository string, item ListAssetsItemResponse) (object fs.Object) {
	// fetch data
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/service/rest/v1/assets/" + item.Id,
		RootURL:    f.opt.Endpoint,
		UserName:   f.opt.Username,
		Password:   f.opt.Password,
		Parameters: url.Values{},
	}
	asset := new(AssetResponse)
	f.client.CallJSON(ctx, &opts, nil, &asset)
	sizeOpts := rest.Opts{
		Method:     "HEAD",
		RootURL:    asset.DownloadUrl,
		Path:       "",
		UserName:   f.opt.Username,
		Password:   f.opt.Password,
		Parameters: url.Values{},
	}
	response, _ := f.client.Call(ctx, &sizeOpts)

	// build Object
	object = &Object{
		remote:  path.Join(repository, item.Path),
		modTime: asset.LastModified,
		checksum: Checksum{
			Md5:  asset.Checksum.Md5,
			Sha1: asset.Checksum.Sha1,
		},
		size: response.ContentLength,
	}
	return object
}

// perform a whole dir walk (walkRDirTree) then uses dirtree to construct expected
// result. DirEntry are copied from DirTree, to allow remote rewrite relative to fs.root
func (f *Fs) List(ctx context.Context, relativeDir string) (entries fs.DirEntries, err error) {
	fs.Debugf(f, "List")
	repository, directory := f.split(relativeDir)
	fullpath := path.Join(repository, directory)
	f.walkRDirTree(ctx, repository)
	cached := (*f.dirtree)[fullpath]
	for _, entry := range cached {
		rel, _ := filepath.Rel(f.root, entry.Remote())
		fs.Debugf(f, "Entry %s", rel)
		if d, ok := entry.(fs.Directory); ok {
			relocated := fs.NewDir(rel, d.ModTime(ctx))
			relocated.SetItems(d.Items())
			entries = append(entries, relocated)
		} else if o, ok := entry.(NexusObject); ok {
			relocated := &Object{
				fs:       f,
				remote:   rel,
				modTime:  o.ModTime(ctx),
				size:     o.Size(),
				checksum: o.Checksum(),
			}
			entries = append(entries, relocated)
		}
	}
	fs.Debugf(f, "%s", entries)
	return
}

// copied and simplified from walk.go ; used to extrapolate dirs from file listing
func (f *Fs) walkRDirTree(ctx context.Context, startPath string) error {
	if f.dirtree != nil {
		return nil
	}
	dirs := dirtree.New()
	// Entries can come in arbitrary order. We use toPrune to keep
	// all directories to exclude later.
	toPrune := make(map[string]bool)
	var mu sync.Mutex
	err := f.listR(ctx, startPath, func(entries fs.DirEntries) error {
		mu.Lock()
		defer mu.Unlock()
		for _, entry := range entries {
			switch x := entry.(type) {
			case fs.Object:
				dirs.Add(x)
			case fs.Directory:
				dirs.AddDir(x)
			default:
				return errors.Errorf("unknown object type %T", entry)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	dirs.CheckParents(startPath)
	if len(dirs) == 0 {
		dirs[startPath] = nil
	}
	err = dirs.Prune(toPrune)
	if err != nil {
		return err
	}
	dirs.Sort()
	f.dirtree = &dirs
	return nil
}

// parentDir finds the parent directory of path
func parentDir(entryPath string) string {
	dirPath := path.Dir(entryPath)
	if dirPath == "." {
		dirPath = ""
	}
	return dirPath
}

type ListCallbackFunc func(item ListAssetsItemResponse) error

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(f, "NewObject")
	repository, _ := f.split(remote)
	f.walkRDirTree(ctx, repository)
	_, i := f.dirtree.Find(remote)
	o, ok := i.(fs.Object)
	if i != nil && ok {
		return o, nil
	} else {
		return nil, fs.ErrorObjectNotFound
	}
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (object fs.Object, err error) {
	fs.Debugf(f, "Put")
	repository, directory := f.split(src.Remote())

	size := src.Size() // NB can upload without size
	var result api.UploadFileResponse
	opts := rest.Opts{
		Method:        "PUT",
		RootURL:       f.opt.Endpoint,
		Path:          "/repository/" + repository + "/" + directory,
		Body:          in,
		ContentType:   fs.MimeType(ctx, src),
		ContentLength: &size,
		Parameters:    url.Values{},
		Options:       options,
		UserName:      f.opt.Username,
		Password:      f.opt.Password,
	}
	_, err = f.client.CallJSON(ctx, &opts, nil, &result)
	f.dirtree = nil
	object = &Object{
		fs:      f,
		remote:  src.Remote(),
		size:    src.Size(),
		modTime: src.ModTime(ctx),
	}
	return object, nil
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
	Sha1 string `json:"sha1"`
	Md5  string `json:"md5"`
}

type AssetResponse struct {
	BlobCreated    time.Time `json:"blobCreated"`
	LastDownloaded time.Time `json:"lastDownloaded"`
	LastModified   time.Time `json:"lastModified"`
	ContentType    string    `json:"contentType"`

	Id          string   `json:"id"`
	Checksum    Checksum `json:"checksum"`
	DownloadUrl string   `json:"downloadUrl"`
	Path        string   `json:"path"`
	Repository  string   `json:"repository"`
	Format      string   `json:"format"`
}

type ListAssetsItemResponse struct {
	Id          string   `json:"id"`
	DownloadUrl string   `json:"downloadUrl"`
	Path        string   `json:"path"`
	Repository  string   `json:"repository"`
	Format      string   `json:"format"`
	Checksum    Checksum `json:"checksum"`
}

type ListAssetsResponse struct {
	Items             []ListAssetsItemResponse `json:"items"`
	ContinuationToken *string                  `json:"continuationToken"`
}

type Object struct {
	fs       *Fs
	checksum Checksum
	remote   string
	modTime  time.Time
	size     int64
}

type NexusObject interface {
	fs.Object

	Checksum() Checksum
}

type CacheItem struct {
	directory *fs.Dir
	file      *Object
	children  []string
}

func (object *Object) String() string {
	return object.remote
}

func (object *Object) Remote() string {
	return object.remote
}

func (object *Object) ModTime(context.Context) time.Time {
	return object.modTime
}

func (object *Object) Size() int64 {
	return object.size
}

func (object *Object) Fs() fs.Info {
	return object.fs
}

func (object *Object) SetModTime(ctx context.Context, t time.Time) error {
	object.modTime = t
	return nil
}

func (object *Object) Checksum() Checksum {
	return object.checksum
}

func (object *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return object.checksum.Md5, nil
}

func (object *Object) Storable() bool {
	return true
}

func (object *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	return nil, nil
}

func (object *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return nil
}

func (object *Object) Remove(ctx context.Context) error {
	return nil
}

var (
	_ fs.Object = (*Object)(nil)
)
