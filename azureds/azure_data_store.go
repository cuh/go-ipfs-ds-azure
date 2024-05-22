package azureds

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// AzureStorage is a storage representation
type AzureStorage struct {
	Config
}

// Config representation for all info needed
type Config struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	FolderName    string
}

// NewAzureDatastore creates an AzureDatastore
func NewAzureDatastore(conf Config) (*AzureStorage, error) {
	credential, err := azblob.NewSharedKeyCredential(conf.AccountName, conf.AccountKey)
	if err != nil {
		return nil, err
	}
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	baseUrl, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", conf.AccountName))
	serviceURL := azblob.NewServiceURL(*baseUrl, pipeline)
	containerURL := serviceURL.NewContainerURL(conf.ContainerName)
	containerURL.Create(context.Background(), azblob.Metadata{}, "")

	return &AzureStorage{
		Config: conf,
	}, nil
}

// GetBlockURL returns the block url of a given key
func (storage *AzureStorage) GetBlockURL(key string) (*azblob.BlockBlobURL, error) {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	accountName := storage.Config.AccountName
	accountKey := storage.Config.AccountKey
	containerName := storage.Config.ContainerName
	folderName := storage.Config.FolderName
	fmt.Printf("accountName:%s\n", accountName)
	fmt.Printf("accountKey:%s\n", accountKey)
	fmt.Printf("containerName:%s\n", containerName)
	fmt.Printf("folderName:%s\n", folderName)
	var string = ""

	// Create a ContainerURL object that wraps a soon-to-be-created blob's URL and a default pipeline.
	if folderName == "" {
		string = fmt.Sprintf("https://%s.blob.core.windows.net/%s%s", accountName, containerName, key)

	} else {
		string = fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s%s", accountName, containerName, folderName, key)
	}
	fmt.Printf("u.string:%s\n", string)
	u, _ := url.Parse(string)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}
	fmt.Printf("azblob.NewSharedKeyCredential ok\n")
	blobURL := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	fmt.Printf("azblob.NewBlockBlobURL ok. %+v\n", blobURL)
	return &blobURL, nil
}

// Put adds a key value pair to the storage
func (storage *AzureStorage) Put(ctx context.Context, k ds.Key, value []byte) error {
	fmt.Printf("azurestorage put:%s\n", k.String())
	fmt.Printf("azurestorage put:%d\n", len(value))
	blobURL, err := storage.GetBlockURL(k.String())
	if err != nil {
		return err
	}

	// Create a blob with metadata (string key/value pairs)
	// NOTE: Metadata key names are always converted to lowercase before being sent to the Storage Service.
	// Therefore, you should always use lowercase letters; especially when querying a map for a metadata key.
	creatingApp, _ := os.Executable()
	fmt.Printf("azurestorage creatingApp:%s\n", creatingApp)
	_, err = blobURL.Upload(ctx, bytes.NewReader(value), azblob.BlobHTTPHeaders{},
		azblob.Metadata{"author": "dqfs", "app": creatingApp}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
	if err != nil {
		return err
	}

	return nil
}

// Sync is unimplemented
func (storage *AzureStorage) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

// Get gets the data from the desired key
func (storage *AzureStorage) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	blobURL, err := storage.GetBlockURL(k.String())
	if err != nil {
		return nil, err
	}

	response, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})

	if err != nil {
		if stgErr, ok := err.(azblob.StorageError); ok {
			if stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
				return nil, ds.ErrNotFound
			}
		}
		return nil, err
	}

	blobData := &bytes.Buffer{}
	reader := response.Body(azblob.RetryReaderOptions{})
	blobData.ReadFrom(reader)
	reader.Close() // The client must close the response body when finished with it
	return blobData.Bytes(), nil
}

// Has checks if the given key exists
func (storage *AzureStorage) Has(ctx context.Context, k ds.Key) (exists bool, err error) {
	blobURL, err := storage.GetBlockURL(k.String())
	if err != nil {
		return false, err
	}

	_, err = blobURL.GetBlockList(ctx, azblob.BlockListCommitted, azblob.LeaseAccessConditions{})
	if err != nil {
		if stgErr, ok := err.(azblob.StorageError); ok &&
			stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetSize gets the size of the specified key
func (storage *AzureStorage) GetSize(ctx context.Context, k ds.Key) (size int, err error) {
	blobURL, err := storage.GetBlockURL(k.String())
	if err != nil {
		return 0, err
	}

	blockList, err := blobURL.GetBlockList(ctx, azblob.BlockListCommitted, azblob.LeaseAccessConditions{})
	if err != nil {
		if stgErr, ok := err.(azblob.StorageError); ok &&
			stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
			return 0, ds.ErrNotFound
		}
		return 0, err
	}
	return int(blockList.BlobContentLength()), nil
}

// Delete deletes the specified key
func (storage *AzureStorage) Delete(ctx context.Context, k ds.Key) error {
	fmt.Printf("\n\nDelete\n")
	fmt.Printf("Delete %s\n", k.String())
	blobURL, err := storage.GetBlockURL(k.String())
	if err != nil {
		return err
	}

	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		fmt.Printf("blob delete err:%+v\n", err)
		if stgErr, ok := err.(azblob.StorageError); ok &&
			stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
			fmt.Printf("blob delete err blob not found\n")
			return ds.ErrNotFound
		}
		return err
	}
	return nil
}

// Query returns a dsq result
func (storage *AzureStorage) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {

	fmt.Printf("Azure Query\nq.String=%v\n", q.String())
	fmt.Printf("q.offset=%v\n", q.Offset)
	fmt.Printf("q.prefix=%v\n", q.Prefix)
	fmt.Printf("q.returnSizes=%v\n", q.ReturnsSizes)
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("Azure: filters or orders are not supported")
	}

	accountName := storage.Config.AccountName
	accountKey := storage.Config.AccountKey
	containerName := storage.Config.ContainerName
	folderName := storage.Config.FolderName

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println("Invalid credentials provided")
		return nil, fmt.Errorf("Azure: Invalid credentials provided")
	}

	limit := q.Limit + q.Offset
	if limit == 0 || limit > 1000 {
		limit = 1000
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	baseUrl, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName))
	serviceURL := azblob.NewServiceURL(*baseUrl, pipeline)

	containerURL := serviceURL.NewContainerURL(containerName)

	var string = ""
	if folderName == "" {
		string = q.Prefix
	} else {
		string = folderName + q.Prefix
	}
	string = strings.TrimPrefix(string, "/")
	string = string + "/"
	fmt.Printf("prefix string=%s\n", string)

	listBlob, err := containerURL.ListBlobsHierarchySegment(context.Background(), azblob.Marker{}, "/", azblob.ListBlobsSegmentOptions{Prefix: string})
	if err != nil {
		fmt.Printf("Error listing blobs in directory: %v\n", err)
		return nil, fmt.Errorf("Azure: Error listing blobs in directory")
	}

	entry := dsq.Entry{}
	fmt.Printf("Azure listblob\n")
	index := q.Offset
	fmt.Printf("len(listBlob.Segment.BlobItems):%d\n", len(listBlob.Segment.BlobItems))
	length := len(listBlob.Segment.BlobItems)
	nextValue := func() (dsq.Result, bool) {
		fmt.Printf("nextValue enter, length=%d\n", length)

		if len(listBlob.Segment.BlobItems) == 0 {
			return dsq.Result{}, false
		}
		if length <= 0 {
			return dsq.Result{}, false
		}

		//for _, blob := range listBlob.Segment.BlobItems {
		blobName := listBlob.Segment.BlobItems[index].Name
		blobSize := listBlob.Segment.BlobItems[index].Properties.ContentLength
		fmt.Printf("length = %d\n", length)
		length = length - 1
		var string = "listbolb:" + ds.NewKey(blobName).String() + "\n"
		fmt.Println(string)

		if strings.HasPrefix(ds.NewKey(blobName).String(), q.Prefix) {
			var string = "key:" + ds.NewKey(blobName).String() + "\n"
			fmt.Println(string)
			fmt.Printf("key size:%d\n", int(*blobSize))
			entry = dsq.Entry{
				Key:   ds.NewKey(blobName).String(),
				Size:  int(*blobSize),
				Value: nil,
			}
			fmt.Printf("dsq entry ok\n")
			index++
			return dsq.Result{Entry: entry}, true
		}
		return dsq.Result{}, false
	}
	fmt.Printf("dsq ResultsFromIterator enter\n")
	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			return nil
		},
		Next: nextValue,
	}), nil
}

// Close is not implemented
func (storage *AzureStorage) Close() error {
	return nil
}

type op struct {
	delete bool
	value  []byte
}

// basicBatch implements
type batchOp struct {
	val    []byte
	delete bool
}

type azureBatch struct {
	storage *AzureStorage
	ops     map[string]batchOp
}

// Batch returns a batch struct that can take more ops or be committed
func (storage *AzureStorage) Batch(ctx context.Context) (ds.Batch, error) {
	return &azureBatch{
		storage: storage,
		ops:     make(map[string]batchOp),
	}, nil
}

func (batch *azureBatch) Put(ctx context.Context, key ds.Key, val []byte) error {
	batch.ops[key.String()] = batchOp{val: val, delete: false}
	return nil
}

func (batch *azureBatch) Delete(ctx context.Context, key ds.Key) error {
	batch.ops[key.String()] = batchOp{val: nil, delete: true}
	return nil
}

func (batch *azureBatch) Commit(ctx context.Context) error {
	var err error
	for k, op := range batch.ops {
		if op.delete {
			err = batch.storage.Delete(ctx, ds.NewKey(k))
		} else {
			err = batch.storage.Put(ctx, ds.NewKey(k), op.val)
		}
		if err != nil {
			break
		}
	}

	return err
}
