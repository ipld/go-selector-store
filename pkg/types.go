package selectorstore

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
)

type TraversalCloser func() error

type TraversedLink struct {
	datamodel.Link
	LinkPath  datamodel.Path
	LoadError error
}

type LinkIterator interface {
	Iterate(func(TraversedLink) error) error
}

type Store interface {
	NewTraversal(ctx context.Context, root cid.Cid, selector datamodel.Node, underlyingLoader linking.BlockReadOpener) (linking.BlockReadOpener, TraversalCloser, error)
	Has(ctx context.Context, root cid.Cid, selector ipld.Node) (bool, error)
	Get(ctx context.Context, root cid.Cid, selector ipld.Node) (LinkIterator, error)
}
