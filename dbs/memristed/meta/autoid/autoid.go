// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package autoid

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// Attention:
// For reading cluster MilevaDB memory blocks, the system schemaReplicant/causet should be same.
// Once the system schemaReplicant/causet id been allocated, it can't be changed any more.
// Change the system schemaReplicant/causet id may have the compatibility problem.
const (
	// SystemSchemaIDFlag is the system schemaReplicant/causet id flag, uses the highest bit position as system schemaReplicant ID flag, it's exports for test.
	SystemSchemaIDFlag = 1 << 62
	// InformationSchemaDBID is the information_schema schemaReplicant id, it's exports for test.
	InformationSchemaDBID int64 = SystemSchemaIDFlag | 1
	// PerformanceSchemaDBID is the performance_schema schemaReplicant id, it's exports for test.
	PerformanceSchemaDBID int64 = SystemSchemaIDFlag | 10000
	// MetricSchemaDBID is the metrics_schema schemaReplicant id, it's exported for test.
	MetricSchemaDBID int64 = SystemSchemaIDFlag | 20000
)

const (
	minStep            = 30000
	maxStep            = 2000000
	defaultConsumeTime = 10 * time.Second
	minIncrement       = 1
	maxIncrement       = 65535
)

// RowIDBitLength is the bit number of a event id in MilevaDB.
const RowIDBitLength = 64

// DefaultAutoRandomBits is the default value of auto sharding.
const DefaultAutoRandomBits = 5

// MaxAutoRandomBits is the max value of auto sharding.
const MaxAutoRandomBits = 15

// Test needs to change it, so it's a variable.
var step = int64(30000)

// SlabPredictorType is the type of allocator for generating auto-id. Different type of allocators use different key-value pairs.
type SlabPredictorType = uint8

const (
	// RowIDAllocType indicates the allocator is used to allocate event id.
	RowIDAllocType SlabPredictorType = iota
	// AutoIncrementType indicates the allocator is used to allocate auto increment value.
	AutoIncrementType
	// AutoRandomType indicates the allocator is used to allocate auto-shard id.
	AutoRandomType
	// SequenceType indicates the allocator is used to allocate sequence value.
	SequenceType
)

// CustomAutoIncCacheOption is one HoTT of AllocOption to customize the allocator step length.
type CustomAutoIncCacheOption int64

// ApplyOn is implement the AllocOption interface.
func (step CustomAutoIncCacheOption) ApplyOn(alloc *allocator) {
	alloc.step = int64(step)
	alloc.customStep = true
}

// AllocOption is a interface to define allocator custom options coming in future.
type AllocOption interface {
	ApplyOn(*allocator)
}

// SlabPredictor is an auto increment id generator.
// Just keep id unique actually.
type SlabPredictor interface {
	// Alloc allocs N consecutive autoID for causet with blockID, returning (min, max] of the allocated autoID batch.
	// It gets a batch of autoIDs at a time. So it does not need to access storage for each call.
	// The consecutive feature is used to insert multiple rows in a memex.
	// increment & offset is used to validate the start position (the allocator's base is not always the last allocated id).
	// The returned range is (min, max]:
	// case increment=1 & offset=1: you can derive the ids like min+1, min+2... max.
	// case increment=x & offset=y: you firstly need to seek to firstID by `SeekToFirstAutoIDXXX`, then derive the IDs like firstID, firstID + increment * 2... in the caller.
	Alloc(blockID int64, n uint64, increment, offset int64) (int64, int64, error)

	// AllocSeqCache allocs sequence batch value cached in causet level（rather than in alloc), the returned range covering
	// the size of sequence cache with it's increment. The returned round indicates the sequence cycle times if it is with
	// cycle option.
	AllocSeqCache(sequenceID int64) (min int64, max int64, round int64, err error)

	// Rebase rebases the autoID base for causet with blockID and the new base value.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	Rebase(blockID, newBase int64, allocIDs bool) error

	// RebaseSeq rebases the sequence value in number axis with blockID and the new base value.
	RebaseSeq(causet, newBase int64) (int64, bool, error)

	// Base return the current base of SlabPredictor.
	Base() int64
	// End is only used for test.
	End() int64
	// NextGlobalAutoID returns the next global autoID.
	NextGlobalAutoID(blockID int64) (int64, error)
	GetType() SlabPredictorType
}

// SlabPredictors represents a set of `SlabPredictor`s.
type SlabPredictors []SlabPredictor

// NewSlabPredictors packs multiple `SlabPredictor`s into SlabPredictors.
func NewSlabPredictors(allocators ...SlabPredictor) SlabPredictors {
	return allocators
}

// Get returns the SlabPredictor according to the SlabPredictorType.
func (all SlabPredictors) Get(allocType SlabPredictorType) SlabPredictor {
	for _, a := range all {
		if a.GetType() == allocType {
			return a
		}
	}
	return nil
}

type allocator struct {
	mu          sync.Mutex
	base        int64
	end         int64
	causetstore ekv.CausetStorage
	// dbID is current database's ID.
	dbID          int64
	isUnsigned    bool
	lastAllocTime time.Time
	step          int64
	customStep    bool
	allocType     SlabPredictorType
	sequence      *perceptron.SequenceInfo
}

// GetStep is only used by tests
func GetStep() int64 {
	return step
}

// SetStep is only used by tests
func SetStep(s int64) {
	step = s
}

// Base implements autoid.SlabPredictor Base interface.
func (alloc *allocator) Base() int64 {
	return alloc.base
}

// End implements autoid.SlabPredictor End interface.
func (alloc *allocator) End() int64 {
	return alloc.end
}

// NextGlobalAutoID implements autoid.SlabPredictor NextGlobalAutoID interface.
func (alloc *allocator) NextGlobalAutoID(blockID int64) (int64, error) {
	var autoID int64
	startTime := time.Now()
	err := ekv.RunInNewTxn(alloc.causetstore, true, func(txn ekv.Transaction) error {
		var err1 error
		m := spacetime.NewMeta(txn)
		autoID, err1 = getAutoIDByAllocType(m, alloc.dbID, blockID, alloc.allocType)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.GlobalAutoID, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if alloc.isUnsigned {
		return int64(uint64(autoID) + 1), err
	}
	return autoID + 1, err
}

func (alloc *allocator) rebase4Unsigned(blockID int64, requiredBase uint64, allocIDs bool) error {
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= uint64(alloc.base) {
		return nil
	}
	// Satisfied by alloc.end, need to uFIDelate alloc.base.
	if requiredBase <= uint64(alloc.end) {
		alloc.base = int64(requiredBase)
		return nil
	}
	var newBase, newEnd uint64
	startTime := time.Now()
	err := ekv.RunInNewTxn(alloc.causetstore, true, func(txn ekv.Transaction) error {
		m := spacetime.NewMeta(txn)
		currentEnd, err1 := getAutoIDByAllocType(m, alloc.dbID, blockID, alloc.allocType)
		if err1 != nil {
			return err1
		}
		uCurrentEnd := uint64(currentEnd)
		if allocIDs {
			newBase = mathutil.MaxUint64(uCurrentEnd, requiredBase)
			newEnd = mathutil.MinUint64(math.MaxUint64-uint64(alloc.step), newBase) + uint64(alloc.step)
		} else {
			if uCurrentEnd >= requiredBase {
				newBase = uCurrentEnd
				newEnd = uCurrentEnd
				// Required base satisfied, we don't need to uFIDelate KV.
				return nil
			}
			// If we don't want to allocate IDs, for example when creating a causet with a given base value,
			// We need to make sure when other MilevaDB server allocates ID for the first time, requiredBase + 1
			// will be allocated, so we need to increase the end to exactly the requiredBase.
			newBase = requiredBase
			newEnd = requiredBase
		}
		_, err1 = generateAutoIDByAllocType(m, alloc.dbID, blockID, int64(newEnd-uCurrentEnd), alloc.allocType)
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.BlockAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = int64(newBase), int64(newEnd)
	return nil
}

func (alloc *allocator) rebase4Signed(blockID, requiredBase int64, allocIDs bool) error {
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= alloc.base {
		return nil
	}
	// Satisfied by alloc.end, need to uFIDelate alloc.base.
	if requiredBase <= alloc.end {
		alloc.base = requiredBase
		return nil
	}
	var newBase, newEnd int64
	startTime := time.Now()
	err := ekv.RunInNewTxn(alloc.causetstore, true, func(txn ekv.Transaction) error {
		m := spacetime.NewMeta(txn)
		currentEnd, err1 := getAutoIDByAllocType(m, alloc.dbID, blockID, alloc.allocType)
		if err1 != nil {
			return err1
		}
		if allocIDs {
			newBase = mathutil.MaxInt64(currentEnd, requiredBase)
			newEnd = mathutil.MinInt64(math.MaxInt64-alloc.step, newBase) + alloc.step
		} else {
			if currentEnd >= requiredBase {
				newBase = currentEnd
				newEnd = currentEnd
				// Required base satisfied, we don't need to uFIDelate KV.
				return nil
			}
			// If we don't want to allocate IDs, for example when creating a causet with a given base value,
			// We need to make sure when other MilevaDB server allocates ID for the first time, requiredBase + 1
			// will be allocated, so we need to increase the end to exactly the requiredBase.
			newBase = requiredBase
			newEnd = requiredBase
		}
		_, err1 = generateAutoIDByAllocType(m, alloc.dbID, blockID, newEnd-currentEnd, alloc.allocType)
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.BlockAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = newBase, newEnd
	return nil
}

// rebase4Sequence won't alloc batch immediately, cause it won't cache value in allocator.
func (alloc *allocator) rebase4Sequence(blockID, requiredBase int64) (int64, bool, error) {
	startTime := time.Now()
	alreadySatisfied := false
	err := ekv.RunInNewTxn(alloc.causetstore, true, func(txn ekv.Transaction) error {
		m := spacetime.NewMeta(txn)
		currentEnd, err := getAutoIDByAllocType(m, alloc.dbID, blockID, alloc.allocType)
		if err != nil {
			return err
		}
		if alloc.sequence.Increment > 0 {
			if currentEnd >= requiredBase {
				// Required base satisfied, we don't need to uFIDelate KV.
				alreadySatisfied = true
				return nil
			}
		} else {
			if currentEnd <= requiredBase {
				// Required base satisfied, we don't need to uFIDelate KV.
				alreadySatisfied = true
				return nil
			}
		}

		// If we don't want to allocate IDs, for example when creating a causet with a given base value,
		// We need to make sure when other MilevaDB server allocates ID for the first time, requiredBase + 1
		// will be allocated, so we need to increase the end to exactly the requiredBase.
		_, err = generateAutoIDByAllocType(m, alloc.dbID, blockID, requiredBase-currentEnd, alloc.allocType)
		return err
	})
	// TODO: sequence metrics
	metrics.AutoIDHistogram.WithLabelValues(metrics.BlockAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return 0, false, err
	}
	if alreadySatisfied {
		return 0, true, nil
	}
	return requiredBase, false, err
}

// Rebase implements autoid.SlabPredictor Rebase interface.
// The requiredBase is the minimum base value after Rebase.
// The real base may be greater than the required base.
func (alloc *allocator) Rebase(blockID, requiredBase int64, allocIDs bool) error {
	if blockID == 0 {
		return errInvalidBlockID.GenWithStack("Invalid blockID")
	}

	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if alloc.isUnsigned {
		return alloc.rebase4Unsigned(blockID, uint64(requiredBase), allocIDs)
	}
	return alloc.rebase4Signed(blockID, requiredBase, allocIDs)
}

// Rebase implements autoid.SlabPredictor RebaseSeq interface.
// The return value is quite same as memex function, bool means whether it should be NULL,
// here it will be used in setval memex function (true meaning the set value has been satisfied, return NULL).
// case1:When requiredBase is satisfied with current value, it will return (0, true, nil),
// case2:When requiredBase is successfully set in, it will return (requiredBase, false, nil).
// If some error occurs in the process, return it immediately.
func (alloc *allocator) RebaseSeq(blockID, requiredBase int64) (int64, bool, error) {
	if blockID == 0 {
		return 0, false, errInvalidBlockID.GenWithStack("Invalid blockID")
	}

	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	return alloc.rebase4Sequence(blockID, requiredBase)
}

func (alloc *allocator) GetType() SlabPredictorType {
	return alloc.allocType
}

// NextStep return new auto id step according to previous step and consuming time.
func NextStep(curStep int64, consumeDur time.Duration) int64 {
	failpoint.Inject("mockAutoIDCustomize", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(3)
		}
	})
	failpoint.Inject("mockAutoIDChange", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(step)
		}
	})

	consumeRate := defaultConsumeTime.Seconds() / consumeDur.Seconds()
	res := int64(float64(curStep) * consumeRate)
	if res < minStep {
		return minStep
	} else if res > maxStep {
		return maxStep
	}
	return res
}

// NewSlabPredictor returns a new auto increment id generator on the causetstore.
func NewSlabPredictor(causetstore ekv.CausetStorage, dbID int64, isUnsigned bool, allocType SlabPredictorType, opts ...AllocOption) SlabPredictor {
	alloc := &allocator{
		causetstore:   causetstore,
		dbID:          dbID,
		isUnsigned:    isUnsigned,
		step:          step,
		lastAllocTime: time.Now(),
		allocType:     allocType,
	}
	for _, fn := range opts {
		fn.ApplyOn(alloc)
	}
	return alloc
}

// NewSequenceSlabPredictor returns a new sequence value generator on the causetstore.
func NewSequenceSlabPredictor(causetstore ekv.CausetStorage, dbID int64, info *perceptron.SequenceInfo) SlabPredictor {
	return &allocator{
		causetstore: causetstore,
		dbID:        dbID,
		// Sequence allocator is always signed.
		isUnsigned:    false,
		lastAllocTime: time.Now(),
		allocType:     SequenceType,
		sequence:      info,
	}
}

// NewSlabPredictorsFromTblInfo creates an array of allocators of different types with the information of perceptron.BlockInfo.
func NewSlabPredictorsFromTblInfo(causetstore ekv.CausetStorage, schemaID int64, tblInfo *perceptron.BlockInfo) SlabPredictors {
	var allocs []SlabPredictor
	dbID := tblInfo.GetDBID(schemaID)
	hasRowID := !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle
	hasAutoIncID := tblInfo.GetAutoIncrementDefCausInfo() != nil
	if hasRowID || hasAutoIncID {
		if tblInfo.AutoIdCache > 0 {
			allocs = append(allocs, NewSlabPredictor(causetstore, dbID, tblInfo.IsAutoIncDefCausUnsigned(), RowIDAllocType, CustomAutoIncCacheOption(tblInfo.AutoIdCache)))
		} else {
			allocs = append(allocs, NewSlabPredictor(causetstore, dbID, tblInfo.IsAutoIncDefCausUnsigned(), RowIDAllocType))
		}
	}
	if tblInfo.ContainsAutoRandomBits() {
		allocs = append(allocs, NewSlabPredictor(causetstore, dbID, tblInfo.IsAutoRandomBitDefCausUnsigned(), AutoRandomType))
	}
	if tblInfo.IsSequence() {
		allocs = append(allocs, NewSequenceSlabPredictor(causetstore, dbID, tblInfo.Sequence))
	}
	return NewSlabPredictors(allocs...)
}

// Alloc implements autoid.SlabPredictor Alloc interface.
// For autoIncrement allocator, the increment and offset should always be positive in [1, 65535].
// Attention:
// When increment and offset is not the default value(1), the return range (min, max] need to
// calculate the correct start position rather than simply the add 1 to min. Then you can derive
// the successive autoID by adding increment * cnt to firstID for (n-1) times.
//
// Example:
// (6, 13] is returned, increment = 4, offset = 1, n = 2.
// 6 is the last allocated value for other autoID or handle, maybe with different increment and step,
// but actually we don't care about it, all we need is to calculate the new autoID corresponding to the
// increment and offset at this time now. To simplify the rule is like (ID - offset) % increment = 0,
// so the first autoID should be 9, then add increment to it to get 13.
func (alloc *allocator) Alloc(blockID int64, n uint64, increment, offset int64) (int64, int64, error) {
	if blockID == 0 {
		return 0, 0, errInvalidBlockID.GenWithStackByArgs("Invalid blockID")
	}
	if n == 0 {
		return 0, 0, nil
	}
	if alloc.allocType == AutoIncrementType || alloc.allocType == RowIDAllocType {
		if !validIncrementAndOffset(increment, offset) {
			return 0, 0, errInvalidIncrementAndOffset.GenWithStackByArgs(increment, offset)
		}
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.isUnsigned {
		return alloc.alloc4Unsigned(blockID, n, increment, offset)
	}
	return alloc.alloc4Signed(blockID, n, increment, offset)
}

func (alloc *allocator) AllocSeqCache(blockID int64) (int64, int64, int64, error) {
	if blockID == 0 {
		return 0, 0, 0, errInvalidBlockID.GenWithStackByArgs("Invalid blockID")
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	return alloc.alloc4Sequence(blockID)
}

func validIncrementAndOffset(increment, offset int64) bool {
	return (increment >= minIncrement && increment <= maxIncrement) && (offset >= minIncrement && offset <= maxIncrement)
}

// CalcNeededBatchSize is used to calculate batch size for autoID allocation.
// It firstly seeks to the first valid position based on increment and offset,
// then plus the length remained, which could be (n-1) * increment.
func CalcNeededBatchSize(base, n, increment, offset int64, isUnsigned bool) int64 {
	if increment == 1 {
		return n
	}
	if isUnsigned {
		// SeekToFirstAutoIDUnSigned seeks to the next unsigned valid position.
		nr := SeekToFirstAutoIDUnSigned(uint64(base), uint64(increment), uint64(offset))
		// Calculate the total batch size needed.
		nr += (uint64(n) - 1) * uint64(increment)
		return int64(nr - uint64(base))
	}
	nr := SeekToFirstAutoIDSigned(base, increment, offset)
	// Calculate the total batch size needed.
	nr += (n - 1) * increment
	return nr - base
}

// CalcSequenceBatchSize calculate the next sequence batch size.
func CalcSequenceBatchSize(base, size, increment, offset, MIN, MAX int64) (int64, error) {
	// The sequence is positive growth.
	if increment > 0 {
		if increment == 1 {
			// Sequence is already allocated to the end.
			if base >= MAX {
				return 0, ErrAutoincReadFailed
			}
			// The rest of sequence < cache size, return the rest.
			if MAX-base < size {
				return MAX - base, nil
			}
			// The rest of sequence is adequate.
			return size, nil
		}
		nr, ok := SeekToFirstSequenceValue(base, increment, offset, MIN, MAX)
		if !ok {
			return 0, ErrAutoincReadFailed
		}
		// The rest of sequence < cache size, return the rest.
		if MAX-nr < (size-1)*increment {
			return MAX - base, nil
		}
		return (nr - base) + (size-1)*increment, nil
	}
	// The sequence is negative growth.
	if increment == -1 {
		if base <= MIN {
			return 0, ErrAutoincReadFailed
		}
		if base-MIN < size {
			return base - MIN, nil
		}
		return size, nil
	}
	nr, ok := SeekToFirstSequenceValue(base, increment, offset, MIN, MAX)
	if !ok {
		return 0, ErrAutoincReadFailed
	}
	// The rest of sequence < cache size, return the rest.
	if nr-MIN < (size-1)*(-increment) {
		return base - MIN, nil
	}
	return (base - nr) + (size-1)*(-increment), nil
}

// SeekToFirstSequenceValue seeks to the next valid value (must be in range of [MIN, MAX]),
// the bool indicates whether the first value is got.
// The seeking formula is describe as below:
//  nr  := (base + increment - offset) / increment
// first := nr*increment + offset
// Because formula computation will overflow Int64, so we transfer it to uint64 for distance computation.
func SeekToFirstSequenceValue(base, increment, offset, MIN, MAX int64) (int64, bool) {
	if increment > 0 {
		// Sequence is already allocated to the end.
		if base >= MAX {
			return 0, false
		}
		uMax := EncodeIntToCmpUint(MAX)
		uBase := EncodeIntToCmpUint(base)
		uOffset := EncodeIntToCmpUint(offset)
		uIncrement := uint64(increment)
		if uMax-uBase < uIncrement {
			// Enum the possible first value.
			for i := uBase + 1; i <= uMax; i++ {
				if (i-uOffset)%uIncrement == 0 {
					return DecodeCmpUintToInt(i), true
				}
			}
			return 0, false
		}
		nr := (uBase + uIncrement - uOffset) / uIncrement
		nr = nr*uIncrement + uOffset
		first := DecodeCmpUintToInt(nr)
		return first, true
	}
	// Sequence is already allocated to the end.
	if base <= MIN {
		return 0, false
	}
	uMin := EncodeIntToCmpUint(MIN)
	uBase := EncodeIntToCmpUint(base)
	uOffset := EncodeIntToCmpUint(offset)
	uIncrement := uint64(-increment)
	if uBase-uMin < uIncrement {
		// Enum the possible first value.
		for i := uBase - 1; i >= uMin; i-- {
			if (uOffset-i)%uIncrement == 0 {
				return DecodeCmpUintToInt(i), true
			}
		}
		return 0, false
	}
	nr := (uOffset - uBase + uIncrement) / uIncrement
	nr = uOffset - nr*uIncrement
	first := DecodeCmpUintToInt(nr)
	return first, true
}

// SeekToFirstAutoIDSigned seeks to the next valid signed position.
func SeekToFirstAutoIDSigned(base, increment, offset int64) int64 {
	nr := (base + increment - offset) / increment
	nr = nr*increment + offset
	return nr
}

// SeekToFirstAutoIDUnSigned seeks to the next valid unsigned position.
func SeekToFirstAutoIDUnSigned(base, increment, offset uint64) uint64 {
	nr := (base + increment - offset) / increment
	nr = nr*increment + offset
	return nr
}

func (alloc *allocator) alloc4Signed(blockID int64, n uint64, increment, offset int64) (int64, int64, error) {
	// Check offset rebase if necessary.
	if offset-1 > alloc.base {
		if err := alloc.rebase4Signed(blockID, offset-1, true); err != nil {
			return 0, 0, err
		}
	}
	// CalcNeededBatchSize calculates the total batch size needed.
	n1 := CalcNeededBatchSize(alloc.base, int64(n), increment, offset, alloc.isUnsigned)

	// Condition alloc.base+N1 > alloc.end will overflow when alloc.base + N1 > MaxInt64. So need this.
	if math.MaxInt64-alloc.base <= n1 {
		return 0, 0, ErrAutoincReadFailed
	}
	// The local rest is not enough for allocN, skip it.
	if alloc.base+n1 > alloc.end {
		var newBase, newEnd int64
		startTime := time.Now()
		nextStep := alloc.step
		if !alloc.customStep {
			// Although it may skip a segment here, we still think it is consumed.
			consumeDur := startTime.Sub(alloc.lastAllocTime)
			nextStep = NextStep(alloc.step, consumeDur)
		}
		err := ekv.RunInNewTxn(alloc.causetstore, true, func(txn ekv.Transaction) error {
			m := spacetime.NewMeta(txn)
			var err1 error
			newBase, err1 = getAutoIDByAllocType(m, alloc.dbID, blockID, alloc.allocType)
			if err1 != nil {
				return err1
			}
			// CalcNeededBatchSize calculates the total batch size needed on global base.
			n1 = CalcNeededBatchSize(newBase, int64(n), increment, offset, alloc.isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := mathutil.MinInt64(math.MaxInt64-newBase, nextStep)
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return ErrAutoincReadFailed
			}
			newEnd, err1 = generateAutoIDByAllocType(m, alloc.dbID, blockID, tmpStep, alloc.allocType)
			return err1
		})
		metrics.AutoIDHistogram.WithLabelValues(metrics.BlockAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err != nil {
			return 0, 0, err
		}
		// CausetStore the step for non-customized-step allocator to calculate next dynamic step.
		if !alloc.customStep {
			alloc.step = nextStep
		}
		alloc.lastAllocTime = time.Now()
		if newBase == math.MaxInt64 {
			return 0, 0, ErrAutoincReadFailed
		}
		alloc.base, alloc.end = newBase, newEnd
	}
	logutil.Logger(context.TODO()).Debug("alloc N signed ID",
		zap.Uint64("from ID", uint64(alloc.base)),
		zap.Uint64("to ID", uint64(alloc.base+n1)),
		zap.Int64("causet ID", blockID),
		zap.Int64("database ID", alloc.dbID))
	min := alloc.base
	alloc.base += n1
	return min, alloc.base, nil
}

func (alloc *allocator) alloc4Unsigned(blockID int64, n uint64, increment, offset int64) (int64, int64, error) {
	// Check offset rebase if necessary.
	if uint64(offset-1) > uint64(alloc.base) {
		if err := alloc.rebase4Unsigned(blockID, uint64(offset-1), true); err != nil {
			return 0, 0, err
		}
	}
	// CalcNeededBatchSize calculates the total batch size needed.
	n1 := CalcNeededBatchSize(alloc.base, int64(n), increment, offset, alloc.isUnsigned)

	// Condition alloc.base+n1 > alloc.end will overflow when alloc.base + n1 > MaxInt64. So need this.
	if math.MaxUint64-uint64(alloc.base) <= uint64(n1) {
		return 0, 0, ErrAutoincReadFailed
	}
	// The local rest is not enough for alloc, skip it.
	if uint64(alloc.base)+uint64(n1) > uint64(alloc.end) {
		var newBase, newEnd int64
		startTime := time.Now()
		nextStep := alloc.step
		if !alloc.customStep {
			// Although it may skip a segment here, we still treat it as consumed.
			consumeDur := startTime.Sub(alloc.lastAllocTime)
			nextStep = NextStep(alloc.step, consumeDur)
		}
		err := ekv.RunInNewTxn(alloc.causetstore, true, func(txn ekv.Transaction) error {
			m := spacetime.NewMeta(txn)
			var err1 error
			newBase, err1 = getAutoIDByAllocType(m, alloc.dbID, blockID, alloc.allocType)
			if err1 != nil {
				return err1
			}
			// CalcNeededBatchSize calculates the total batch size needed on new base.
			n1 = CalcNeededBatchSize(newBase, int64(n), increment, offset, alloc.isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := int64(mathutil.MinUint64(math.MaxUint64-uint64(newBase), uint64(nextStep)))
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return ErrAutoincReadFailed
			}
			newEnd, err1 = generateAutoIDByAllocType(m, alloc.dbID, blockID, tmpStep, alloc.allocType)
			return err1
		})
		metrics.AutoIDHistogram.WithLabelValues(metrics.BlockAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err != nil {
			return 0, 0, err
		}
		// CausetStore the step for non-customized-step allocator to calculate next dynamic step.
		if !alloc.customStep {
			alloc.step = nextStep
		}
		alloc.lastAllocTime = time.Now()
		if uint64(newBase) == math.MaxUint64 {
			return 0, 0, ErrAutoincReadFailed
		}
		alloc.base, alloc.end = newBase, newEnd
	}
	logutil.Logger(context.TODO()).Debug("alloc unsigned ID",
		zap.Uint64(" from ID", uint64(alloc.base)),
		zap.Uint64("to ID", uint64(alloc.base+n1)),
		zap.Int64("causet ID", blockID),
		zap.Int64("database ID", alloc.dbID))
	min := alloc.base
	// Use uint64 n directly.
	alloc.base = int64(uint64(alloc.base) + uint64(n1))
	return min, alloc.base, nil
}

// alloc4Sequence is used to alloc value for sequence, there are several aspects different from autoid logic.
// 1: sequence allocation don't need check rebase.
// 2: sequence allocation don't need auto step.
// 3: sequence allocation may have negative growth.
// 4: sequence allocation batch length can be dissatisfied.
// 5: sequence batch allocation will be consumed immediately.
func (alloc *allocator) alloc4Sequence(blockID int64) (min int64, max int64, round int64, err error) {
	increment := alloc.sequence.Increment
	offset := alloc.sequence.Start
	minValue := alloc.sequence.MinValue
	maxValue := alloc.sequence.MaxValue
	cacheSize := alloc.sequence.CacheValue
	if !alloc.sequence.Cache {
		cacheSize = 1
	}

	var newBase, newEnd int64
	startTime := time.Now()
	err = ekv.RunInNewTxn(alloc.causetstore, true, func(txn ekv.Transaction) error {
		m := spacetime.NewMeta(txn)
		var (
			err1    error
			seqStep int64
		)
		// Get the real offset if the sequence is in cycle.
		// round is used to count cycle times in sequence with cycle option.
		if alloc.sequence.Cycle {
			// GetSequenceCycle is used to get the flag `round`, which indicates whether the sequence is already in cycle.
			round, err1 = m.GetSequenceCycle(alloc.dbID, blockID)
			if err1 != nil {
				return err1
			}
			if round > 0 {
				if increment > 0 {
					offset = alloc.sequence.MinValue
				} else {
					offset = alloc.sequence.MaxValue
				}
			}
		}

		// Get the global new base.
		newBase, err1 = getAutoIDByAllocType(m, alloc.dbID, blockID, alloc.allocType)
		if err1 != nil {
			return err1
		}

		// CalcNeededBatchSize calculates the total batch size needed.
		seqStep, err1 = CalcSequenceBatchSize(newBase, cacheSize, increment, offset, minValue, maxValue)

		if err1 != nil && err1 == ErrAutoincReadFailed {
			if !alloc.sequence.Cycle {
				return err1
			}
			// Reset the sequence base and offset.
			if alloc.sequence.Increment > 0 {
				newBase = alloc.sequence.MinValue - 1
				offset = alloc.sequence.MinValue
			} else {
				newBase = alloc.sequence.MaxValue + 1
				offset = alloc.sequence.MaxValue
			}
			err1 = m.SetSequenceValue(alloc.dbID, blockID, newBase)
			if err1 != nil {
				return err1
			}

			// Reset sequence round state value.
			round++
			// SetSequenceCycle is used to causetstore the flag `round` which indicates whether the sequence is already in cycle.
			// round > 0 means the sequence is already in cycle, so the offset should be minvalue / maxvalue rather than sequence.start.
			// MilevaDB is a stateless node, it should know whether the sequence is already in cycle when restart.
			err1 = m.SetSequenceCycle(alloc.dbID, blockID, round)
			if err1 != nil {
				return err1
			}

			// Recompute the sequence next batch size.
			seqStep, err1 = CalcSequenceBatchSize(newBase, cacheSize, increment, offset, minValue, maxValue)
			if err1 != nil {
				return err1
			}
		}
		var delta int64
		if alloc.sequence.Increment > 0 {
			delta = seqStep
		} else {
			delta = -seqStep
		}
		newEnd, err1 = generateAutoIDByAllocType(m, alloc.dbID, blockID, delta, alloc.allocType)
		return err1
	})

	// TODO: sequence metrics
	metrics.AutoIDHistogram.WithLabelValues(metrics.BlockAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return 0, 0, 0, err
	}
	logutil.Logger(context.TODO()).Debug("alloc sequence value",
		zap.Uint64(" from value", uint64(newBase)),
		zap.Uint64("to value", uint64(newEnd)),
		zap.Int64("causet ID", blockID),
		zap.Int64("database ID", alloc.dbID))
	return newBase, newEnd, round, nil
}

func getAutoIDByAllocType(m *spacetime.Meta, dbID, blockID int64, allocType SlabPredictorType) (int64, error) {
	switch allocType {
	// Currently, event id allocator and auto-increment value allocator shares the same key-value pair.
	case RowIDAllocType, AutoIncrementType:
		return m.GetAutoBlockID(dbID, blockID)
	case AutoRandomType:
		return m.GetAutoRandomID(dbID, blockID)
	case SequenceType:
		return m.GetSequenceValue(dbID, blockID)
	default:
		return 0, ErrInvalidSlabPredictorType.GenWithStackByArgs()
	}
}

func generateAutoIDByAllocType(m *spacetime.Meta, dbID, blockID, step int64, allocType SlabPredictorType) (int64, error) {
	switch allocType {
	case RowIDAllocType, AutoIncrementType:
		return m.GenAutoBlockID(dbID, blockID, step)
	case AutoRandomType:
		return m.GenAutoRandomID(dbID, blockID, step)
	case SequenceType:
		return m.GenSequenceValue(dbID, blockID, step)
	default:
		return 0, ErrInvalidSlabPredictorType.GenWithStackByArgs()
	}
}

const signMask uint64 = 0x8000000000000000

// EncodeIntToCmpUint make int v to comparable uint type
func EncodeIntToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask
}

// DecodeCmpUintToInt decodes the u that encoded by EncodeIntToCmpUint
func DecodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

// TestModifyBaseAndEndInjection exported for testing modifying the base and end.
func TestModifyBaseAndEndInjection(alloc SlabPredictor, base, end int64) {
	alloc.(*allocator).mu.Lock()
	alloc.(*allocator).base = base
	alloc.(*allocator).end = end
	alloc.(*allocator).mu.Unlock()
}

// AutoRandomIDLayout is used to calculate the bits length of different section in auto_random id.
// The primary key with auto_random can only be `bigint` defCausumn, the total layout length of auto random is 64 bits.
// These are two type of layout:
// 1. Signed bigint:
//   | [sign_bit] | [shard_bits] | [incremental_bits] |
//   sign_bit(1 fixed) + shard_bits(15 max) + incremental_bits(the rest) = total_layout_bits(64 fixed)
// 2. Unsigned bigint:
//   | [shard_bits] | [incremental_bits] |
//   shard_bits(15 max) + incremental_bits(the rest) = total_layout_bits(64 fixed)
// Please always use NewAutoRandomIDLayout() to instantiate.
type AutoRandomIDLayout struct {
	FieldType *types.FieldType
	ShardBits uint64
	// Derived fields.
	TypeBitsLength  uint64
	IncrementalBits uint64
	HasSignBit      bool
}

// NewAutoRandomIDLayout create an instance of AutoRandomIDLayout.
func NewAutoRandomIDLayout(fieldType *types.FieldType, shardBits uint64) *AutoRandomIDLayout {
	typeBitsLength := uint64(allegrosql.DefaultLengthOfMysqlTypes[allegrosql.TypeLonglong] * 8)
	incrementalBits := typeBitsLength - shardBits
	hasSignBit := !allegrosql.HasUnsignedFlag(fieldType.Flag)
	if hasSignBit {
		incrementalBits -= 1
	}
	return &AutoRandomIDLayout{
		FieldType:       fieldType,
		ShardBits:       shardBits,
		TypeBitsLength:  typeBitsLength,
		IncrementalBits: incrementalBits,
		HasSignBit:      hasSignBit,
	}
}

// IncrementalBitsCapacity returns the max capacity of incremental section of the current layout.
func (l *AutoRandomIDLayout) IncrementalBitsCapacity() uint64 {
	return uint64(math.Pow(2, float64(l.IncrementalBits))) - 1
}

// IncrementalMask returns 00..0[11..1], where [xxx] is the incremental section of the current layout.
func (l *AutoRandomIDLayout) IncrementalMask() int64 {
	return (1 << l.IncrementalBits) - 1
}
