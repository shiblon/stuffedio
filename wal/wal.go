// Package wal implements a full write-ahead log over a directory of files,
// including snapshot management, reply iterators, and rotating log writers.
package wal

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"entrogo.com/stuffedio/orderedio"
	"entrogo.com/stuffedio/recordio"
)

const (
	DefaultJournalBase  = "journal"
	DefaultSnapshotBase = "snapshot"
	DefaultMaxIndices   = 10 * 1 << 10 // 10Ki
	DefaultMaxBytes     = 10 * 1 << 20 // 10Mi
	OldPrefix           = "_old__"
	PartPrefix          = "_partial__"
	FinalSuffix         = "final"
)

var (
	indexPattern = regexp.MustCompile(`^([a-fA-F0-9]+)-([^.-]+)(?:-(` + FinalSuffix + `))?$`)
)

// Loader is a function type called by snapshot item loads and journal entry replay.
type Loader func([]byte) error

// Option describes a WAL creation option.
type Option func(*WAL)

// WithRequireEmpty can be used to indicate that this is a brand new WAL and it
// must have zero files in it when it is initially opened. Can be used with
// WithAllowWrite if desired. Implies that empty loaders and players are
// allowed.
func WithRequireEmpty(e bool) Option {
	return func(w *WAL) {
		w.requireInitiallyEmpty = e
		if e {
			w.emptyLoader = true
			w.emptyPlayer = true
			w.excludeLive = true
		}
	}
}

// WithJournalBase sets the journal base, otherwise uses DefaultJournalBase.
func WithJournalBase(p string) Option {
	return func(w *WAL) {
		w.journalBase = p
	}
}

// WithSnaphotBase sets the snapshot base, otherwise uses DefaultSnapshotBase.
func WithSnapshotBase(p string) Option {
	return func(w *WAL) {
		w.snapshotBase = p
	}
}

// WithSnapshotLoader sets the record adder for all snapshot records. Clients
// provide one of these to allow snapshots to be loaded.
func WithSnapshotLoader(a Loader) Option {
	return func(w *WAL) {
		w.snapshotLoader = a
	}
}

// WithJournalPlayer sets the journal player for all journal records after the
// latest snapshot. Clients provide this to allow journals to be replayed.
func WithJournalPlayer(p Loader) Option {
	return func(w *WAL) {
		w.journalPlayer = p
	}
}

// WithMaxJournalBytes sets the maximum number of bytes before a journal is rotated.
// Default is DefaultMaxBytes.
func WithMaxJournalBytes(m int64) Option {
	return func(w *WAL) {
		w.maxJournalBytes = m
	}
}

// WithMaxJournalIndices sets the maximum number of indices in a journal file
// before it must be rotated.
func WithMaxJournalIndices(m int) Option {
	return func(w *WAL) {
		w.maxJournalIndices = m
	}
}

// WithAllowWrite lets the WAL go into "write" mode after it has been parsed.
// The default is to be read-only, and to create errors when attempting to do
// write operations on the file system. This makes it easy to not make
// mistakes, for example, when trying to collect journals to make a new
// snapshot. The WAL can be opened in read-only mode, a snapshot can be
// created, then it can be reopened in write mode and it will know to load that
// snapshot instead of replaying the entire set of journals. An empty snapshot
// loader can be given in that case to speed the loading process.
func WithAllowWrite(a bool) Option {
	return func(w *WAL) {
		w.allowWrite = a
	}
}

// WithEmptySnapshotLoader indicates that not loading a snapshot is
// actually desired. This is to prevent mistakes: usually a snapshot adder is
// wanted, if there is a snapshot to be added.
func WithEmptySnapshotLoader(a bool) Option {
	return func(w *WAL) {
		w.emptyLoader = a
	}
}

// WithEmptyJournalPlayer indicates that journals are to be scanned, not processed.
// This is a safety measure to avoid default behavior being unwanted: usually
// you want to process journal entries, but sometimes there is good reason to
// simply scan for the proper index and start appending.
func WithEmptyJournalPlayer(a bool) Option {
	return func(w *WAL) {
		w.emptyPlayer = a
	}
}

// WithExcludeLiveJournal indicates that only "final" journals should be played. Implies writing disallowed. It is an error to specify this with WithAllowWrite.
func WithExcludeLiveJournal(e bool) Option {
	return func(w *WAL) {
		w.excludeLive = e
	}
}

// WAL implements a write-ahead logger capable of replaying snapshots and
// journals, setting up a writer for appending to journals and rotating them
// when full, etc.
type WAL struct {
	dir               string
	journalBase       string
	snapshotBase      string
	maxJournalBytes   int64
	maxJournalIndices int

	emptyLoader           bool
	emptyPlayer           bool
	allowWrite            bool
	excludeLive           bool
	requireInitiallyEmpty bool

	loadedAJournal  bool
	loadedASnapshot bool

	snapshotLoader Loader
	journalPlayer  Loader

	snapshotWasLast bool // If the snapshot was the last thing read (no later journals).

	currSize    int64              // Current size of current journal file.
	currCount   int                // Current number of indices in current journal file.
	currMeta    *FileMeta          // Current info about the live journal.
	nextIndex   uint64             // Keep track of what the next record's index should be.
	currEncoder *orderedio.Encoder // The current encoder, can be rotated on write.
}

// FileMeta contains information about a file entry in the log directory.
type FileMeta struct {
	Name    string
	Base    string
	Index   uint64
	IsFinal bool
	IsOld   bool
}

// String outputs friendly info for this file meta.
func (m *FileMeta) String() string {
	fStr := "f"
	oStr := "o"
	if !m.IsFinal {
		fStr = "!f"
	}
	if !m.IsOld {
		oStr = "!o"
	}
	return fmt.Sprintf("%s=(%s:%d:%s:%s)", m.Name, m.Base, m.Index, fStr, oStr)
}

type dirInfo struct {
	oldSnapshots []*FileMeta
	oldJournals  []*FileMeta

	partSnapshots []*FileMeta
	snapshots     []*FileMeta

	journals     []*FileMeta
	liveJournals []*FileMeta
}

func (d *dirInfo) String() string {
	var lines []string
	lines = append(lines, fmt.Sprintf("oldSnapshots: %q", d.oldSnapshots))
	lines = append(lines, fmt.Sprintf("oldJournals: %q", d.oldJournals))
	lines = append(lines, fmt.Sprintf("partSnapshots: %q", d.partSnapshots))
	lines = append(lines, fmt.Sprintf("snapshots: %q", d.snapshots))
	lines = append(lines, fmt.Sprintf("journals: %q", d.journals))
	lines = append(lines, fmt.Sprintf("liveJournals: %q", d.liveJournals))
	return "dirInfo:\n\t" + strings.Join(lines, "\n\t")
}

func (d *dirInfo) hasFiles() bool {
	return len(d.oldSnapshots) != 0 || len(d.oldJournals) != 0 || len(d.partSnapshots) != 0 || len(d.snapshots) != 0 || len(d.journals) != 0 || len(d.liveJournals) != 0
}

// Open opens a directory and loads the WAL found in it, then provides a WAL
// that can be appended to over time.
func Open(dir string, opts ...Option) (*WAL, error) {
	w := &WAL{
		dir: dir,

		journalBase:       DefaultJournalBase,
		maxJournalBytes:   DefaultMaxBytes,
		maxJournalIndices: DefaultMaxIndices,
		snapshotBase:      DefaultSnapshotBase,
	}
	for _, opt := range opts {
		opt(w)
	}

	if w.excludeLive && w.allowWrite {
		return nil, fmt.Errorf("wal open configuration: can't allow writes while excluding the final live journal")
	}
	if w.emptyLoader && w.snapshotLoader != nil {
		return nil, fmt.Errorf("wal open configuration: can't specify both empty and non-empty snapshot adder")
	}
	if w.emptyPlayer && w.journalPlayer != nil {
		return nil, fmt.Errorf("wal open configuration: can't specify both empty and non-empty journal player")
	}

	fsys := os.DirFS(dir)

	dInf, err := w.openDir(fsys)
	if err != nil {
		return nil, fmt.Errorf("wal open file meta: %w", err)
	}
	if w.requireInitiallyEmpty && dInf.hasFiles() {
		return nil, fmt.Errorf("wal open configuration: empty initial dir required, but files found in %q", dir)
	}
	if err := w.deprecateOldFiles(dInf); err != nil {
		return nil, fmt.Errorf("wal open deprecate: %w", err)
	}

	snapshotOK, err := w.loadSnapshot(fsys, dInf)
	if err != nil {
		return nil, fmt.Errorf("wal open snapshot: %w", err)
	}
	w.loadedASnapshot = snapshotOK

	journalOK, err := w.playJournals(fsys, dInf, w.excludeLive)
	if err != nil {
		return nil, fmt.Errorf("wal open journals: %w", err)
	}
	w.loadedAJournal = journalOK

	// If nothing was read (snapshot or journal), set up for a correct first index.
	if !snapshotOK && !journalOK {
		w.nextIndex = 1
	}
	if !w.allowWrite {
		// Read-only - don't open the last file for writing.
		return w, nil
	}

	if err := w.maybeInitLiveJournal(dInf); err != nil {
		return nil, fmt.Errorf("open wal init live: %w", err)
	}

	return w, nil
}

func (w *WAL) maybeInitLiveJournal(inf *dirInfo) error {
	if w.excludeLive || !w.allowWrite {
		return fmt.Errorf("init live invalid request: exclude=%v, write=%v", w.excludeLive, w.allowWrite)
	}
	if live, liveOK := inf.liveJournalToContinue(); liveOK {
		f, err := os.OpenFile(filepath.Join(w.dir, live.Name), os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("init live from file: %w", err)
		}
		w.currMeta = live
		w.currEncoder = orderedio.NewEncoder(
			recordio.NewEncoder(f),
			orderedio.WithFirstIndex(w.nextIndex),
		)
		return nil
	}

	// No live journal file, time to make one. We can do that by rotating. We
	// have a nil currEncoder, and the last index of the last thing read is
	// known.
	if err := w.rotate(); err != nil {
		return fmt.Errorf("init live new rotate: %w", err)
	}

	return nil
}

func (w *WAL) openDir(fsys fs.FS) (*dirInfo, error) {
	ents, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("catalog files: %w", err)
	}
	inf := new(dirInfo)
	for _, ent := range ents {
		meta, err := ParseIndexName(ent.Name())
		if err != nil {
			log.Printf("Unrecognized file pattern for %q, skipping", ent.Name())
			continue
		}
		switch meta.Base {
		case w.journalBase:
			if !meta.IsFinal {
				inf.liveJournals = append(inf.liveJournals, meta)
			} else if meta.IsOld {
				inf.oldJournals = append(inf.oldJournals, meta)
			} else {
				inf.journals = append(inf.journals, meta)
			}
		case w.snapshotBase:
			if !meta.IsFinal {
				inf.partSnapshots = append(inf.partSnapshots, meta)
			} else if meta.IsOld {
				inf.oldSnapshots = append(inf.oldSnapshots, meta)
			} else {
				inf.snapshots = append(inf.snapshots, meta)
			}
		default:
			return nil, fmt.Errorf("Unknown base in %q", meta.Name)
		}
	}

	sortMeta := func(s []*FileMeta) {
		sort.Slice(s, func(i, j int) bool { return s[i].Name < s[j].Name })
	}

	sortMeta(inf.oldSnapshots)
	sortMeta(inf.oldJournals)
	sortMeta(inf.partSnapshots)
	sortMeta(inf.snapshots)
	sortMeta(inf.journals)
	sortMeta(inf.liveJournals)

	if err := inf.checkValid(); err != nil {
		return nil, fmt.Errorf("invalid dir info: %w", err)
	}

	return inf, nil
}

func (d *dirInfo) checkValid() error {
	if snap, ok := d.snapshotToLoad(); ok {
		if js := d.journalsToPlay(); len(js) != 0 {
			if got, want := js[0].Index, snap.Index+1; want != got {
				return fmt.Errorf("first journal has index %d, wanted 1 greater than snapshot %d", got, want)
			}
		}
	}
	return nil
}

func (d *dirInfo) firstUsefulJournal() int {
	snap, ok := d.snapshotToLoad()
	if !ok {
		// No snapshot, first useful journal is the first journal.
		return 0
	}
	for i, j := range d.journals {
		if j.Index > snap.Index {
			return i
		}
	}
	return len(d.journals) // no useful journals, return one beyond the end.
}

func (d *dirInfo) snapshotToLoad() (*FileMeta, bool) {
	if len(d.snapshots) == 0 {
		return nil, false
	}
	return d.snapshots[len(d.snapshots)-1], true
}

func (d *dirInfo) snapshotsToDeprecate() []*FileMeta {
	if len(d.snapshots) < 2 {
		return nil
	}
	return d.snapshots[:len(d.snapshots)-1]
}

func (d *dirInfo) journalsToPlay() []*FileMeta {
	return d.journals[d.firstUsefulJournal():]
}

func (d *dirInfo) journalsToDeprecate() []*FileMeta {
	return d.journals[:d.firstUsefulJournal()]
}

func (d *dirInfo) liveJournalToContinue() (*FileMeta, bool) {
	if len(d.liveJournals) == 0 {
		return nil, false
	}
	j := d.liveJournals[len(d.liveJournals)-1]
	if snap, ok := d.snapshotToLoad(); ok && j.Index <= snap.Index {
		return nil, false
	}
	prevJs := d.journalsToPlay()
	if len(prevJs) != 0 && prevJs[len(prevJs)-1].Index >= j.Index {
		return nil, false
	}

	return j, true
}

func (w *WAL) deprecateOldFiles(inf *dirInfo) error {
	if !w.allowWrite {
		return nil // ignore if not allowed to write
	}
	mv := func(name string) error {
		return os.Rename(filepath.Join(w.dir, name), filepath.Join(w.dir, OldPrefix+name))
	}

	for _, m := range inf.snapshotsToDeprecate() {
		if err := mv(m.Name); err != nil {
			return fmt.Errorf("open wal deprecate snapshots: %w", err)
		}
	}

	for _, m := range inf.journalsToDeprecate() {
		if err := mv(m.Name); err != nil {
			return fmt.Errorf("open wal deprecate journals: %w", err)
		}
	}
	return nil
}

func (w *WAL) loadSnapshot(fsys fs.FS, inf *dirInfo) (bool, error) {
	snapshot, snapshotOK := inf.snapshotToLoad()
	if !snapshotOK {
		return false, nil
	}

	// Only allow snapshot load to be skipped if explicitly asked.
	if !w.emptyLoader && w.snapshotLoader == nil {
		return false, fmt.Errorf("open wal: snapshot found but no snapshot adder option given")
	}

	// Snapshot indicates which journal index should come next.
	w.nextIndex = snapshot.Index + 1

	// Skip reading snapshot if requested.
	if w.snapshotLoader == nil {
		return true, nil
	}

	f, err := fsys.Open(snapshot.Name)
	if err != nil {
		return false, fmt.Errorf("open wal open snapshot: %w", err)
	}
	dec := orderedio.NewDecoder(recordio.NewDecoder(f))
	for !dec.Done() {
		idx, b, err := dec.Next()
		if err != nil {
			return false, fmt.Errorf("open wal snapshot next (%d): %w", idx, err)
		}
		if err := w.snapshotLoader(b); err != nil {
			return false, fmt.Errorf("open wal snapshot add (%d): %w", idx, err)
		}
	}
	return true, nil
}

func (w *WAL) playJournals(fsys fs.FS, inf *dirInfo, excludeLive bool) (bool, error) {
	toPlay := inf.journalsToPlay()
	if live, ok := inf.liveJournalToContinue(); !excludeLive && ok {
		toPlay = append(toPlay, live)
	}

	if len(toPlay) == 0 {
		return false, nil
	}

	// Note that we don't use the files iterator and multi decoder because
	// we need filenames all the way along the process, to check that indices match.
	// So we implement some of the loops here by hand instead.
	for _, m := range toPlay {
		name := m.Name

		w.currCount = 0
		if !w.emptyPlayer && w.journalPlayer == nil {
			return false, fmt.Errorf("play journal: journal files found but no journal player option given")
		}
		f, err := fsys.Open(name)
		if err != nil {
			return false, fmt.Errorf("play journal open: %w", err)
		}
		fi, err := f.Stat()
		if err != nil {
			return false, fmt.Errorf("play journal stat: %w", err)
		}
		w.currSize = fi.Size()

		dec := orderedio.NewDecoder(recordio.NewDecoder(f))
		defer dec.Close()

		checked := false
		for !dec.Done() {
			idx, b, err := dec.Next()
			if err != nil {
				return false, fmt.Errorf("play journal next: %w", err)
			}
			if w.nextIndex == 0 {
				w.nextIndex = idx
			}
			if w.nextIndex != idx {
				return false, fmt.Errorf("play journal next: want index %d, got %d", w.nextIndex, idx)
			}
			w.nextIndex++
			w.currCount++
			if !checked {
				checked = true
				if err := CheckIndexName(name, w.journalBase, idx); err != nil {
					return false, fmt.Errorf("play journal check: %w", err)
				}
			}
			if w.journalPlayer != nil {
				if err := w.journalPlayer(b); err != nil {
					return false, fmt.Errorf("play journal: %w", err)
				}
			}
		}
	}
	return true, nil
}

// Append sends another record to the journal, and can trigger rotation of underlying files.
func (w *WAL) Append(b []byte) error {
	if !w.allowWrite {
		return fmt.Errorf("wal append: not opened for appending, read-only")
	}
	if w.currEncoder == nil {
		return fmt.Errorf("wal append: no current journal encoder")
	}
	if w.timeToRotate() {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("append rotate if ready: %v", err)
		}
	}
	n, err := w.currEncoder.Encode(w.nextIndex, b)
	if err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	w.currSize += int64(n)
	w.currCount++
	w.nextIndex++
	return nil
}

// CurrIndex returns index number for the most recently read (or written) journal entry.
func (w *WAL) CurrIndex() uint64 {
	if w.nextIndex == 0 {
		return 0
	}
	return w.nextIndex - 1
}

// Close cleans up any open resources. Live journals are left live, however,
// for next time. To ensure that live journals get finalized, call Finalize.
func (w *WAL) Close() error {
	if w.currEncoder != nil {
		err := w.currEncoder.Close()
		w.currEncoder = nil
		return err
	}
	return nil
}

// Finalize causes an open live journal to become final, so it will not be
// appended to again. After completion, writes are disabled, but snapshots can be created.
func (w *WAL) Finalize() error {
	if !w.allowWrite {
		return fmt.Errorf("wal finalize: can't finalize a read-only log")
	}
	if w.currEncoder == nil {
		return fmt.Errorf("wal finalize: no live journal to finalize")
	}

	if err := w.finalizeLiveJournal(); err != nil {
		return fmt.Errorf("wal finalize: %w", err)
	}

	w.allowWrite = false

	return nil
}

// timeToRotate returns whether we are due for a rotation.
func (w *WAL) timeToRotate() (yes bool) {
	if w.currSize >= w.maxJournalBytes {
		return true
	}
	if w.currCount >= w.maxJournalIndices {
		return true
	}
	return false
}

// finalizeLiveJournal closes the current journal, if any, and renames it to be final.
func (w *WAL) finalizeLiveJournal() error {
	if !w.allowWrite {
		return fmt.Errorf("finalize impl: cannot finalize if writing is not allowed")
	}
	if w.currEncoder == nil {
		return nil
	}
	if w.currMeta == nil {
		return fmt.Errorf("finalize impl: encoder available, but meta not - should never happen")
	}
	err := w.currEncoder.Close()
	liveName := filepath.Join(w.dir, w.currMeta.Name)
	w.currEncoder = nil
	w.currMeta = nil
	if err := os.Rename(liveName, liveName+"-"+FinalSuffix); err != nil {
		return fmt.Errorf("rotate rename live: %w", err)
	}
	return err
}

// rotate performs a file rotation, closing the current encoder and opening a
// new one over a new file, if possible.
func (w *WAL) rotate() error {
	if !w.allowWrite {
		return fmt.Errorf("wal rotate: not opened for append, read-only")
	}
	defer func() {
		w.currCount = 0
		w.currSize = 0
	}()
	if err := w.finalizeLiveJournal(); err != nil {
		return fmt.Errorf("rotate: %w", err)
	}
	// Open new.
	live := &FileMeta{
		Name:  IndexName(w.journalBase, w.nextIndex),
		Base:  w.journalBase,
		Index: w.nextIndex,
	}
	f, err := os.Create(filepath.Join(w.dir, live.Name))
	if err != nil {
		return fmt.Errorf("rotate new live: %w", err)
	}
	w.currMeta = live
	w.currEncoder = orderedio.NewEncoder(
		recordio.NewEncoder(f),
		orderedio.WithFirstIndex(w.nextIndex),
	)
	return nil
}

// IndexName returns a string for the given index value.
func IndexName(base string, idx uint64) string {
	return fmt.Sprintf("%016x-%s", idx, base)
}

// CheckIndexName checkes that the given file name contains the right base and index.
func CheckIndexName(name, base string, index uint64) error {
	meta, err := ParseIndexName(name)
	if err != nil {
		return fmt.Errorf("check index name %q: %w", name, err)
	}
	if meta.Index != index {
		return fmt.Errorf("check index name %q: data index is %d, but filename index is %d", name, index, meta.Index)
	}
	if meta.Base != base {
		return fmt.Errorf("check index name %q: desired base %q, but filename base is %q", name, base, meta.Base)
	}
	return nil
}

// ParseIndexName pulls the index from a file name. Should not have path components.
func ParseIndexName(name string) (*FileMeta, error) {
	isOld := strings.HasPrefix(name, OldPrefix)
	isPart := strings.HasPrefix(name, PartPrefix)

	checkName := name

	if isOld {
		checkName = name[len(OldPrefix):]
	}
	if isPart {
		checkName = name[len(PartPrefix):]
	}

	groups := indexPattern.FindStringSubmatch(checkName)
	if len(groups) == 0 {
		return nil, fmt.Errorf("parse name %q: no match for %q", name, checkName)
	}
	idxStr, base, finalSuffix := groups[1], groups[2], groups[3]
	isFinal := finalSuffix == FinalSuffix

	idx, err := strconv.ParseUint(idxStr, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("parse name: %w", err)
	}
	return &FileMeta{
		Name:    name,
		Base:    base,
		Index:   idx,
		IsOld:   isOld,
		IsFinal: isFinal,
	}, nil
}

// CanSnapshot indicates whether a snapshot can be taken, ignoring the actual reason.
// Calls CheckCanSnapshot underneath. If a reason error is desired, use CheckCanSnapshot.
func (w *WAL) CanSnapshot() bool {
	return w.CheckCanSnapshot() == nil
}

// CheckCanSnapshot indicates whether a snapshot can be taken, returning a
// non-nil error with an appropriate message if not. In
// particular, if no journals have been loaded, either because the last thing
// found was a snapshot, or because there simply aren't any, it usually doesn't
// make sense to create a snapshot. One exception to this is if no journals or
// snapshots are loaded at all, indicating an effectively empty journal
// directory. In this case, it is allowed to create a snapshot (e.g., to seed a
// new WAL with values already known).

func (w *WAL) CheckCanSnapshot() error {
	if w.allowWrite {
		return fmt.Errorf("wal check snapshot: snapshots can't be taken on writable WAL")
	}
	if !w.excludeLive {
		return fmt.Errorf("wal check snapshot: snapshots can't be taken when live journals are in the data")
	}
	if !w.loadedAJournal && w.loadedASnapshot {
		return fmt.Errorf("wal check snapshot: last thing read was a snapshot, it doesn't make sense to take one")
	}
	return nil
}

// ValueAdder describes the interface passed to snapshot creation functions,
// allowing them to add entries to a snapshot while important scaffolding is
// handled behind the scenes..
type ValueAdder interface {
	AddValue([]byte) error
}

type journalEncoderAdder struct {
	encoder *orderedio.Encoder
	index   uint64
}

// AddValue adds a value to a snapshot.
func (a *journalEncoderAdder) AddValue(b []byte) error {
	if _, err := a.encoder.Encode(a.index+1, b); err != nil {
		return fmt.Errorf("snapshot add value: %w", err)
	}
	a.index++
	return nil
}

// Snapshotter receives a ValuerAdder that it can use to provide values. If it
// returns a non-nil error, the snapshot will not be finalized.
type Snapshotter func(ValueAdder) error

// CreateSnapshot creates a snapshot encoder for writing. Checks whether it can
// proceed first, returning an error if it is not sensible to create a
// snapshot. Call CanSnapshot first to look before you leap.
func (w *WAL) CreateSnapshot(s Snapshotter) (string, error) {
	if err := w.CheckCanSnapshot(); err != nil {
		return "", fmt.Errorf("wal snapshot: %w", err)
	}
	liveName := filepath.Join(w.dir, IndexName(w.snapshotBase, w.nextIndex))
	f, err := os.OpenFile(liveName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return "", fmt.Errorf("create snapshot: %w", err)
	}
	adder := &journalEncoderAdder{
		encoder: orderedio.NewEncoder(recordio.NewEncoder(f)),
	}
	if err := s(adder); err != nil {
		return "", fmt.Errorf("create snapshot: %w", err)
	}
	// No error, close this thing and rename it.
	adder.encoder.Close()
	finalName := liveName + "-" + FinalSuffix
	if err := os.Rename(liveName, finalName); err != nil {
		return "", fmt.Errorf("create snapshot finalize: %w", err)
	}
	return finalName, nil
}
