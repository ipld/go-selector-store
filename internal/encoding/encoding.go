package encoding

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/ipld/go-car/util"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	selectorstore "github.com/ipld/go-selector-store/pkg"
	mh "github.com/multiformats/go-multihash"
)

// ToDsKey converts a cid+selector to a go-datastore key
func ToDsKey(root cid.Cid, selector ipld.Node) (datastore.Key, error) {
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

// FromDsKey reads in a CID+selector from a go-datastore key, using the given
// codec type for the CID
func FromDsKey(codecType uint64, key datastore.Key) (cid.Cid, ipld.Node, error) {
	data, err := dshelp.BinaryFromDsKey(key)
	if err != nil {
		return cid.Undef, nil, err
	}
	buf := bytes.NewReader(data)
	mhr := mh.NewReader(buf)
	hash, err := mhr.ReadMultihash()
	c := cid.NewCidV1(codecType, hash)
	if err != nil {
		return cid.Undef, nil, err
	}
	builder := basicnode.Prototype.Any.NewBuilder()
	err = dagcbor.Decode(builder, buf)
	if err != nil {
		return cid.Undef, nil, err
	}
	nd := builder.Build()
	return c, nd, nil
}

// EncodeTraversedLink writes a traversed link to a store
// writes:
// varint for length of all +
// cid (contains length) + path len varint + path string as bytes + error string (nothing for nil error)
func EncodeTraversedLink(out io.Writer, traversedLink selectorstore.TraversedLink) error {
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

// DecodeTraversedLink reads a traversed link data structure from a stream
func DecodeTraversedLink(in *bufio.Reader) (selectorstore.TraversedLink, error) {
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
