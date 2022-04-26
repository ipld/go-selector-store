# go-selector-store

### So far...

I defined a simple interface for a selector store. 

After several iterations for how to do writing, I settled on a call that takes cid + selector + a linking.BlockReadOpener (i.e. the link loading function) and returns a wrapped link.BlockReadOpener that memoizes the traversal as it loads links, and a commit function that says "I'm done, write it to the data store

This seems the most flexible especially since now in go-graphsync we actually do weird stuff like loading additional links in the context of calling the visit function.

The selector store also has a Has call and a Get call that returns an Iterator you can use to get all the links traversed in a traversal

The selector store is found in `pkg/types.go`

I wrote a super simple, untested implementation that works on top of go-datastore. For now, I simply make the datastore keys cid.Multihash + dagcbor encoded selector. I encode traversed links in a very simple varint encoded format that cribs some functionality from go-car, though also I wonder if we should make the traversed link just an ipld struct we encode with cbor. 

The simple selector store implementation is in `pkg/simple/simple.go`

Next steps are:
1. Test the simple implementation and explore optimizations
2. Looking at storing this data in a more structured way -- one thing that would be super cool if a write for a cid + recursive all selector could serve data for all queries for the all-selector as long as the cid was any cid inside the graph of the first cid + selector combo, for example.