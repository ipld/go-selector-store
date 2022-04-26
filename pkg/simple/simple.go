package simple

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	util "github.com/ipld/go-car/util"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorstore "github.com/ipld/go-selector-store/pkg"
	mh "github.com/multiformats/go-multihash"
)

type simpleSelectorStore struct {
	ds datastore.Datastore
}

func NewSimpleSelectorStore(ds datastore.Datastore) selectorstore.Store {
	return &simpleSelectorStore{
		ds: ds,
	}
}

// toDsKey converts a cid+selector to a go-datastore key
func toDsKey(root cid.Cid, selector ipld.Node) (datastore.Key, error) {
	buf := new(bytes.Buffer)
	// write the multihash of the cid
	mhw := mh.NewWriter(buf)
	err := mhw.WriteMultihash(root.Hash())
	if err != nil {
		return datastore.Key{}, err
	}
	// encode the selector as cbor
	err = dagcbor.Encode(selector, mhw)
	if err != nil {
		return datastore.Key{}, err
	}
	data := buf.Bytes()
	// return byte buffer encoded to ds key
	return dshelp.NewKeyFromBinary(data), nil
}

// encodeTraversedLink writes a traversed link to a store
// writes:
// varint for length of all +
// cid (contains length) + path len varint + path string as bytes + error string (nothing for nil error)
func encodeTraversedLink(out io.Writer, traversedLink selectorstore.TraversedLink) error {
	// get cid bytes (cid encodes length on its own)
	cidBytes := traversedLink.Link.(cidlink.Link).Bytes()
	// write the path to a byte arrach
	pathBytes := []byte(traversedLink.LinkPath.String())
	// encode path length as varint
	pathLenBytes := make([]byte, binary.MaxVarintLen64)
	written := binary.PutUvarint(pathLenBytes, uint64(len(pathBytes)))
	pathLenBytes = pathLenBytes[:written]
	// encode error string as bytes, do not write nil error
	var errBytes []byte
	if traversedLink.LoadError != nil {
		errBytes = []byte(traversedLink.LoadError.Error())
	}
	// return
	return util.LdWrite(out, cidBytes, pathLenBytes, pathBytes, errBytes)
}

// decodeTraversedLink reads a traversed link data structure from a stream
func decodeTraversedLink(in *bufio.Reader) (selectorstore.TraversedLink, error) {
	data, err := util.LdRead(in)
	if err != nil {
		return selectorstore.TraversedLink{}, err
	}
	c, n, err := util.ReadCid(data)
	if err != nil {
		return selectorstore.TraversedLink{}, err
	}
	data = data[n:]
	pathLen, n := binary.Uvarint(data)
	if n <= 0 {
		return selectorstore.TraversedLink{}, errors.New("could not read pathLen")
	}
	data = data[n:]
	path := datamodel.ParsePath(string(data[:pathLen]))
	data = data[pathLen:]
	var linkErr error
	if len(data) > 0 {
		linkErr = errors.New(string(data))
	}
	return selectorstore.TraversedLink{cidlink.Link{Cid: c}, path, linkErr}, nil
}

// linkIterator implements the selectorstore.LinkIterator interface
type linkIterator struct {
	reader *bufio.Reader
}

func (li *linkIterator) Iterate(processLink func(selectorstore.TraversedLink) error) error {
	for {
		nextTraversedLink, err := decodeTraversedLink(li.reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		err = processLink(nextTraversedLink)
		if err != nil {
			return err
		}
	}
}

type traversalWriter struct {
	ctx              context.Context
	underlyingLoader linking.BlockReadOpener
	out              *bytes.Buffer
	writeErr         error
	key              datastore.Key
	ds               datastore.Datastore
}

func (tw *traversalWriter) load(linkCtx linking.LinkContext, link datamodel.Link) (io.Reader, error) {
	// encoding errors prevent future loads
	if tw.writeErr != nil {
		return nil, tw.writeErr
	}
	reader, err := tw.underlyingLoader(linkCtx, link)
	traversedLink := selectorstore.TraversedLink{
		Link:      link,
		LinkPath:  linkCtx.LinkPath,
		LoadError: err,
	}
	encodeErr := encodeTraversedLink(tw.out, traversedLink)
	// record encodeErr
	if encodeErr != nil {
		tw.writeErr = encodeErr
	}
	return reader, err
}

func (tw *traversalWriter) commit() error {
	if tw.writeErr != nil {
		return tw.writeErr
	}
	bufBytes := tw.out.Bytes()
	return tw.ds.Put(tw.ctx, tw.key, bufBytes)
}

func (sss *simpleSelectorStore) NewTraversal(ctx context.Context, root cid.Cid, selector datamodel.Node, underlyingLoader linking.BlockReadOpener) (linking.BlockReadOpener, selectorstore.TraversalCloser, error) {
	key, err := toDsKey(root, selector)
	if err != nil {
		return nil, nil, err
	}
	tw := &traversalWriter{ctx, underlyingLoader, new(bytes.Buffer), nil, key, sss.ds}
	return tw.load, tw.commit, nil
}

func (sss *simpleSelectorStore) Has(ctx context.Context, root cid.Cid, selector ipld.Node) (bool, error) {
	key, err := toDsKey(root, selector)
	if err != nil {
		return false, err
	}
	return sss.ds.Has(ctx, key)
}

func (sss *simpleSelectorStore) Get(ctx context.Context, root cid.Cid, selector ipld.Node) (selectorstore.LinkIterator, error) {
	key, err := toDsKey(root, selector)
	if err != nil {
		return nil, err
	}
	linkBytes, err := sss.ds.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return &linkIterator{bufio.NewReader(bytes.NewReader(linkBytes))}, nil
}
