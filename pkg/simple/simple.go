package simple

import (
	"bufio"
	"bytes"
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-selector-store/internal/encoding"
	selectorstore "github.com/ipld/go-selector-store/pkg"
)

type simpleSelectorStore struct {
	ds datastore.Datastore
}

func NewSimpleSelectorStore(ds datastore.Datastore) selectorstore.Store {
	return &simpleSelectorStore{
		ds: ds,
	}
}

// linkIterator implements the selectorstore.LinkIterator interface
type linkIterator struct {
	reader *bufio.Reader
}

func (li *linkIterator) Iterate(processLink func(selectorstore.TraversedLink) error) error {
	for {
		nextTraversedLink, err := encoding.DecodeTraversedLink(li.reader)
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
	encodeErr := encoding.EncodeTraversedLink(tw.out, traversedLink)
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
	key, err := encoding.ToDsKey(root, selector)
	if err != nil {
		return nil, nil, err
	}
	tw := &traversalWriter{ctx, underlyingLoader, new(bytes.Buffer), nil, key, sss.ds}
	return tw.load, tw.commit, nil
}

func (sss *simpleSelectorStore) Has(ctx context.Context, root cid.Cid, selector ipld.Node) (bool, error) {
	key, err := encoding.ToDsKey(root, selector)
	if err != nil {
		return false, err
	}
	return sss.ds.Has(ctx, key)
}

func (sss *simpleSelectorStore) Get(ctx context.Context, root cid.Cid, selector ipld.Node) (selectorstore.LinkIterator, error) {
	key, err := encoding.ToDsKey(root, selector)
	if err != nil {
		return nil, err
	}
	linkBytes, err := sss.ds.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return &linkIterator{bufio.NewReader(bytes.NewReader(linkBytes))}, nil
}
