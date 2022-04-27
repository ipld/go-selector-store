package encoding_test

import (
	"bufio"
	"bytes"
	"errors"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipld/go-selector-store/internal/encoding"
	selectorstore "github.com/ipld/go-selector-store/pkg"
	"github.com/ipld/go-selector-store/testutil"
	"github.com/stretchr/testify/require"
)

func TestDSKeyRoundTrip(t *testing.T) {
	testCids := testutil.GenerateCids(5)
	selectors := []datamodel.Node{selectorparse.CommonSelector_ExploreAllRecursively, selectorparse.CommonSelector_MatchPoint, selectorparse.CommonSelector_MatchAllRecursively}
	for _, c := range testCids {
		for _, sel := range selectors {
			key, err := encoding.ToDsKey(c, sel)
			require.NoError(t, err)
			decodedCid, decodedSelector, err := encoding.FromDsKey(cid.DagProtobuf, key)
			require.NoError(t, err)
			require.Equal(t, c.Hash(), decodedCid.Hash())
			require.Equal(t, sel, decodedSelector)
		}
	}
}

func TestTraversedLinkRoundTrip(t *testing.T) {
	testCids := testutil.GenerateCids(5)
	testCases := map[string]struct {
		path    datamodel.Path
		loadErr error
	}{
		"no error": {
			path:    datamodel.ParsePath("2/apples"),
			loadErr: nil,
		},
		"error": {
			path:    datamodel.ParsePath("apples/4/cheese/contained"),
			loadErr: errors.New("Something went wrong"),
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			for _, c := range testCids {
				traversedLink := selectorstore.TraversedLink{
					Link:      cidlink.Link{c},
					LinkPath:  data.path,
					LoadError: data.loadErr,
				}
				buf := new(bytes.Buffer)
				err := encoding.EncodeTraversedLink(buf, traversedLink)
				require.NoError(t, err)
				input := bufio.NewReader(buf)
				finalLink, err := encoding.DecodeTraversedLink(input)
				require.NoError(t, err)
				require.Equal(t, traversedLink, finalLink)
			}
		})
	}
}
