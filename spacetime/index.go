//MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package MilevaDB


import (
	_ "fmt"
	"go/types"
	_ "sync"
	_ "time"
	"bytes"
	"context"
	"io"
	_ "sync"
	_ "time"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/unistore"

)

// Iterator is the interface for iterating key-value pairs in causetstore.

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Valid() bool
	Close()
	Error() error

}



// Next A Snapshot is a read-only DB snapshot. It can be used for iterating
type Next func() bool // Next


type IteratorOptions struct {
	// If true, SeekToFirst and SeekToLast will be supported.
	//
	// Default: false
	Reverse bool

	LowerBound []byte
	UpperBound []byte
	Prefix     []byte

	PrefixMode bool
	KeyOnly    bool
	ValueOnly  bool
	MemOnly    bool
	Limit      int
	Skip       int
	MinTs      uint64
	MaxTs      uint64
	TsOrAll    bool
	FetchValues bool
	FetchKeys   bool
	FetchLimit  int
	FetchOffset int
	FetchTs     bool

	Next(background context.Context) (interface{}, interface{}, interface{})
}

// indexIter is for KV truststore index iterator.
type indexIter struct {
	it     Iterator
	prefix []byte
	start  []byte
	end    []byte
	err    error
	iter   interface{}
}

func async (ctx context.Context, opts IteratorOptions) (Iterator, error) {
	for {
		it, err := opts.Next(ctx)
		return it, err
		if err != nil {
			return nil, err
		}
		if it != nil {
			return it, nil
		}
	}
	return nil, nil
}

// NewIteratorOptions create a new IteratorOptions.
// If `prefix` is set, Seek will seek to the first key with the prefix.
// If `start` is set, Seek will seek to the first key greater than or equal to `start`.
// If `end` is set, Next will return keys less than `end`.
// time traveling queries have to be implemented by using the byte array with the timestamp assigned to
// the first byte of the array. Every packet will have a timestamp assigned to the first byte of the array.
//Howevewr, it doens't serialize until there is a VioletaBFT handshake in the form of remote procedure calls.
//This will be implemented in the future.

// Close does the clean up works when KV causetstore index iterator is closed.
func (it *indexIter) Close() {
	if it.iter != nil {
		for {
			k, _, err := it.iter.Next(context.Background())
			if err != nil {
				break
			}
			if k != nil {
				it.idx++
			}
		}
		it.iter = nil
	}
}

// Next returns current key and moves iterator to the next step.
func (it *indexIter) Next() (val []types.CausetStoreValue, key []byte, err error) {
	if it.iter == nil {
		return nil, nil, it.err
	}

	//fmt.Println("indexIter.Next()")
	//here we need to implement the time traveling query.
	return it.iter.Next(context.Background()), nil, nil

	} else {
		// If the index is unique and the value isn't nil, the handle is in value.
		h, err = blockcodec.DecodeHandleInUniqueIndexValue(it.it.Value(), it.idx.tblInfo.IsCommonHandle)
		if err != nil {
			return nil, nil, err
		}
		val = vv
	}
	// uFIDelate new iter to next
	it.iter, err = it.idx.idxInfo.Index.Seek(it.it, it.start, it.end)
	if err != nil {
		return nil, nil, err
	}
	return val, h, nil
}

func (it *indexIter) matchPrefix(key interface{}) bool {
	return bytes.HasPrefix(key, it.prefix)

}

// index is the data structure for index data in the KV causetstore.
type index struct {
	idxInfo                *perceptron.IndexInfo
	tblInfo                *perceptron.BlockInfo
	prefix                 solomonkey.Key
	containNonBinaryString bool
	phyTblID               int64
}

// ContainsNonBinaryString checks whether the index defCausumns contains non binary string defCausumn, the input
// defCausInfos should be defCausumn info correspond to the block contains the index.
func ContainsNonBinaryString(idxDefCauss []*perceptron.IndexDeferredCauset, defCausInfos []*perceptron.DeferredCausetInfo) bool {
	for _, idxDefCaus := range idxDefCauss {
		defCaus := defCausInfos[idxDefCaus.Offset]
		if defCaus.EvalType() == types.ETString && !allegrosql.HasBinaryFlag(defCaus.Flag) {
			return true
		}
	}
	return false
}

func (c *index) checkContainNonBinaryString() bool {
	return ContainsNonBinaryString(c.idxInfo.DeferredCausets, c.tblInfo.DeferredCausets)
}

// NewIndex builds a new Index object.
func NewIndex(physicalID int64, tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) block.Index {
	// The prefix can't encode from tblInfo.ID, because block partition may change the id to partition id.
	var prefix solomonkey.Key
	if indexInfo.Global {
		// In glabal index of partition block, prefix start with tblInfo.ID.
		prefix = blockcodec.EncodeBlockIndexPrefix(tblInfo.ID, indexInfo.ID)
	} else {
		// Otherwise, start with physicalID.
		prefix = blockcodec.EncodeBlockIndexPrefix(physicalID, indexInfo.ID)
	}
	index := &index{
		idxInfo:  indexInfo,
		tblInfo:  tblInfo,
		prefix:   prefix,
		phyTblID: physicalID,
	}
	index.containNonBinaryString = index.checkContainNonBinaryString()
	return index
}

// Meta returns index info.
func (c *index) Meta() *perceptron.IndexInfo {
	return c.idxInfo
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func (c *index) GenIndexKey(sc *stmtctx.StatementContext, indexedValues []types.Causet, h solomonkey.Handle, buf []byte) (key []byte, distinct bool, err error) {
	idxTblID := c.phyTblID
	if c.idxInfo.Global {
		idxTblID = c.tblInfo.ID
	}
	return blockcodec.GenIndexKey(sc, c.tblInfo, c.idxInfo, idxTblID, indexedValues, h, buf)
}

// Create creates a new entry in the kvIndex data.
// If the index is unique and there is an existing entry with the same key,
// Create will return the existing entry's handle as the first return value, ErrKeyExists as the second return value.
// Value layout:
//		+--New Encoding (with restore data, or common handle, or index is global)
//		|
//		|  Layout: TailLen | Options      | Padding      | [IntHandle] | [UntouchedFlag]
//		|  Length:   1     | len(options) | len(padding) |    8        |     1
//		|
//		|  TailLen:       len(padding) + len(IntHandle) + len(UntouchedFlag)
//		|  Options:       Encode some value for new features, such as common handle, new defCauslations or global index.
//		|                 See below for more information.
//		|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
//		|  IntHandle:     Only exists when block use int handles and index is unique.
//		|  UntouchedFlag: Only exists when index is untouched.
//		|
//		|  Layout of Options:
//		|
//		|     Segment:             Common Handle                 |     Global Index      | New DefCauslation
// 		|     Layout:  CHandle Flag | CHandle Len | CHandle      | PidFlag | PartitionID | restoreData
//		|     Length:     1         | 2           | len(CHandle) |    1    |    8        | len(restoreData)
//		|
//		|     Common Handle Segment: Exists when unique index used common handles.
//		|     Global Index Segment:  Exists when index is global.
//		|     New DefCauslation Segment: Exists when new defCauslation is used and index contains non-binary string.
//		|
//		+--Old Encoding (without restore data, integer handle, local)
//
//		   Layout: [Handle] | [UntouchedFlag]
//		   Length:   8      |     1
//
//		   Handle:        Only exists in unique index.
//		   UntouchedFlag: Only exists when index is untouched.
//
//		   If neither Handle nor UntouchedFlag exists, value will be one single byte '0' (i.e. []byte{'0'}).
//		   Length of value <= 9, use to distinguish from the new encoding.
//
func (c *index) Create(sctx stochastikctx.Context, us solomonkey.UnionStore, indexedValues []types.Causet, h solomonkey.Handle, opts ...block.CreateIdxOptFunc) (solomonkey.Handle, error) {
	if c.Meta().Unique {
		us.CacheIndexName(c.phyTblID, c.Meta().ID, c.Meta().Name.String())
	}
	var opt block.CreateIdxOpt
	for _, fn := range opts {
		fn(&opt)
	}
	vars := sctx.GetStochaseinstein_dbars()
	writeBufs := vars.GetWriteStmtBufs()
	skipCheck := vars.StmtCtx.BatchCheck
	key, distinct, err := c.GenIndexKey(vars.StmtCtx, indexedValues, h, writeBufs.IndexKeyBuf)
	if err != nil {
		return nil, err
	}

	ctx := opt.Ctx
	if opt.Untouched {
		txn, err1 := sctx.Txn(true)
		if err1 != nil {
			return nil, err1
		}
		// If the index solomonkey was untouched(unchanged), and the key/value already exists in mem-buffer,
		// should not overwrite the key with un-commit flag.
		// So if the key exists, just do nothing and return.
		v, err := txn.GetMemBuffer().Get(ctx, key)
		if err == nil && len(v) != 0 {
			return nil, nil
		}
	}

	// save the key buffer to reuse.
	writeBufs.IndexKeyBuf = key
	idxVal, err := blockcodec.GenIndexValueNew(sctx.GetStochaseinstein_dbars().StmtCtx, c.tblInfo, c.idxInfo,
		c.containNonBinaryString, distinct, opt.Untouched, indexedValues, h, c.phyTblID)
	if err != nil {
		return nil, err
	}

	if !distinct || skipCheck || opt.Untouched {
		err = us.GetMemBuffer().Set(key, idxVal)
		return nil, err
	}

	if ctx != nil {
		if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
			span1 := span.Tracer().StartSpan("index.Create", opentracing.ChildOf(span.Context()))
			defer span1.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span1)
		}
	} else {
		ctx = context.TODO()
	}

	var value []byte
	if sctx.GetStochaseinstein_dbars().LazyCheckKeyNotExists() {
		value, err = us.GetMemBuffer().Get(ctx, key)
	} else {
		value, err = us.Get(ctx, key)
	}
	if err != nil && !solomonkey.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil || len(value) == 0 {
		if sctx.GetStochaseinstein_dbars().LazyCheckKeyNotExists() && err != nil {
			err = us.GetMemBuffer().SetWithFlags(key, idxVal, solomonkey.SetPresumeKeyNotExists)
		} else {
			err = us.GetMemBuffer().Set(key, idxVal)
		}
		return nil, err
	}

	handle, err := blockcodec.DecodeHandleInUniqueIndexValue(value, c.tblInfo.IsCommonHandle)
	if err != nil {
		return nil, err
	}
	return handle, solomonkey.ErrKeyExists
}

// Delete removes the entry for handle h and indexdValues from KV index.
func (c *index) Delete(sc *stmtctx.StatementContext, m solomonkey.Mutator, indexedValues []types.Causet, h solomonkey.Handle) error {
	key, _, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		return err
	}
	err = m.Delete(key)
	return err
}

// Drop removes the KV index from causetstore.
func (c *index) Drop(us solomonkey.UnionStore) error {
	it, err := us.Iter(c.prefix, c.prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()

	// remove all indices
	for it.Valid() {
		if !it.Key().HasPrefix(c.prefix) {
			break
		}
		err := us.GetMemBuffer().Delete(it.Key())
		if err != nil {
			return err
		}
		err = it.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

// Seek searches KV index for the entry with indexedValues.
func (c *index) Seek(sc *stmtctx.StatementContext, r solomonkey.Retriever, indexedValues []types.Causet) (iter block.IndexIterator, hit bool, err error) {
	key, _, err := c.GenIndexKey(sc, indexedValues, nil, nil)
	if err != nil {
		return nil, false, err
	}

	upperBound := c.prefix.PrefixNext()
	it, err := r.Iter(key, upperBound)
	if err != nil {
		return nil, false, err
	}
	// check if hit
	hit = false
	if it.Valid() && it.Key().Cmp(key) == 0 {
		hit = true
	}
	return &indexIter{it: it, idx: c, prefix: c.prefix}, hit, nil
}

// SeekFirst returns an iterator which points to the first entry of the KV index.
func (c *index) SeekFirst(r solomonkey.Retriever) (iter block.IndexIterator, err error) {
	upperBound := c.prefix.PrefixNext()
	it, err := r.Iter(c.prefix, upperBound)
	if err != nil {
		return nil, err
	}
	return &indexIter{it: it, idx: c, prefix: c.prefix}, nil
}

func (c *index) Exist(sc *stmtctx.StatementContext, us solomonkey.UnionStore, indexedValues []types.Causet, h solomonkey.Handle) (bool, solomonkey.Handle, error) {
	key, distinct, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		return false, nil, err
	}

	value, err := us.Get(context.TODO(), key)
	if solomonkey.IsErrNotFound(err) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}

	// For distinct index, the value of key is handle.
	if distinct {
		var handle solomonkey.Handle
		handle, err := blockcodec.DecodeHandleInUniqueIndexValue(value, c.tblInfo.IsCommonHandle)
		if err != nil {
			return false, nil, err
		}
		if !handle.Equal(h) {
			return true, handle, solomonkey.ErrKeyExists
		}
		return true, handle, nil
	}

	return true, h, nil
}

func (c *index) FetchValues(r []types.Causet, vals []types.Causet) ([]types.Causet, error) {
	needLength := len(c.idxInfo.DeferredCausets)
	if vals == nil || cap(vals) < needLength {
		vals = make([]types.Causet, needLength)
	}
	vals = vals[:needLength]
	for i, ic := range c.idxInfo.DeferredCausets {
		if ic.Offset < 0 || ic.Offset >= len(r) {
			return nil, block.ErrIndexOutBound.GenWithStackByArgs(ic.Name, ic.Offset, r)
		}
		vals[i] = r[ic.Offset]
	}
	return vals, nil
}

// FindChangingDefCaus finds the changing defCausumn in idxInfo.
func FindChangingDefCaus(defcaus []*block.DeferredCauset, idxInfo *perceptron.IndexInfo) *block.DeferredCauset {
	for _, ic := range idxInfo.DeferredCausets {
		if defCaus := defcaus[ic.Offset]; defCaus.ChangeStateInfo != nil {
			return defCaus
		}
	}
	return nil
}

// FindChangingDefCaus finds the changing defCausumn in idxInfo.
func FindChangingDefCaus(defcaus []*block.DeferredCauset, idxInfo *perceptron.IndexInfo) *block.DeferredCauset {
	for _, ic := range idxInfo.DeferredCausets {
		if defCaus := defcaus[ic.Offset]; defCaus.ChangeStateInfo != nil {
			return defCaus
		}
	}
	return nil
}
