package simple_test

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	selectorstore "github.com/ipld/go-selector-store/pkg"
	"github.com/ipld/go-selector-store/pkg/simple"
	"github.com/ipld/go-selector-store/testutil"
	"github.com/stretchr/testify/require"
)

func TestWriteSelector(t *testing.T) {
	testTree := testutil.NewTestIPLDTree()
	lsys := testutil.NewTestStore(testTree.Storage)
	testCases := map[string]struct {
		root                   cid.Cid
		sel                    datamodel.Node
		expectedTraversedLinks []selectorstore.TraversedLink
	}{
		"explore all recursively from root": {
			root: testTree.RootBlock.Cid(),
			sel:  selectorparse.CommonSelector_ExploreAllRecursively,
			expectedTraversedLinks: []selectorstore.TraversedLink{
				{
					Link:      testTree.RootNodeLnk,
					LinkPath:  datamodel.ParsePath(""),
					LoadError: nil,
				},
				{
					Link:      testTree.MiddleListNodeLnk,
					LinkPath:  datamodel.ParsePath("linkedList"),
					LoadError: nil,
				},
				{
					Link:      testTree.LeafAlphaLnk,
					LinkPath:  datamodel.ParsePath("linkedList/0"),
					LoadError: nil,
				},
				{
					Link:      testTree.LeafAlphaLnk,
					LinkPath:  datamodel.ParsePath("linkedList/1"),
					LoadError: nil,
				},
				{
					Link:      testTree.LeafBetaLnk,
					LinkPath:  datamodel.ParsePath("linkedList/2"),
					LoadError: nil,
				},
				{
					Link:      testTree.LeafAlphaLnk,
					LinkPath:  datamodel.ParsePath("linkedList/3"),
					LoadError: nil,
				},
				{
					Link:      testTree.MiddleMapNodeLnk,
					LinkPath:  datamodel.ParsePath("linkedMap"),
					LoadError: nil,
				},
				{
					Link:      testTree.LeafAlphaLnk,
					LinkPath:  datamodel.ParsePath("linkedMap/nested/alink"),
					LoadError: nil,
				},
				{
					Link:      testTree.LeafAlphaLnk,
					LinkPath:  datamodel.ParsePath("linkedString"),
					LoadError: nil,
				},
			},
		},
	}
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	store := simple.NewSimpleSelectorStore(ds)
	ctx := context.Background()
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			newOpener, closer, err := store.NewTraversal(ctx, data.root, data.sel, lsys.StorageReadOpener)
			require.NoError(t, err)
			newLsys := lsys
			newLsys.StorageReadOpener = newOpener
			nd, err := newLsys.Load(
				linking.LinkContext{Ctx: ctx, LinkPath: datamodel.NewPath(nil)},
				cidlink.Link{Cid: data.root},
				basicnode.Prototype.Any)
			require.NoError(t, err)
			compiled, err := selector.CompileSelector(data.sel)
			require.NoError(t, err)
			err = traversal.Progress{
				Cfg: &traversal.Config{
					Ctx:                            ctx,
					LinkSystem:                     newLsys,
					LinkTargetNodePrototypeChooser: basicnode.Chooser,
				},
			}.WalkAdv(nd, compiled, func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error { return nil })
			require.NoError(t, err)
			err = closer()
			require.NoError(t, err)
			has, err := store.Has(ctx, data.root, data.sel)
			require.NoError(t, err)
			require.True(t, has)
			iter, err := store.Get(ctx, data.root, data.sel)
			require.NoError(t, err)
			i := 0
			iter.Iterate(func(traversedLink selectorstore.TraversedLink) error {
				require.Less(t, i, len(data.expectedTraversedLinks))
				require.Equal(t, data.expectedTraversedLinks[i], traversedLink)
				i++
				return nil
			})
			require.Len(t, data.expectedTraversedLinks, i)
		})
	}
}
