package segment

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"vdo-cmps/pkg/utils/erasure"
	"vdo-cmps/pkg/utils/hash"
	"vdo-cmps/pkg/utils/merklet"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"

	"github.com/pkg/errors"
)

const SEGMENT_SIZE = cesspat.SegmentSize
const FRAG_SHARD_SIZE = cesspat.DataShards + cesspat.ParShards

type Fragment struct {
	Hash               *hash.H256 `json:"hash"`
	Index              int        `json:"index"`
	belongSegmentIndex int        `json:"-"`
	pathMgr            *pathMgr   `json:"-"`
}

type Segment struct {
	Hash  *hash.H256 `json:"hash"`
	Index int        `json:"index"`
	Frags []Fragment `json:"frags"`
}

type FileSegmentMeta struct {
	InputSize int64      `json:"inputSize"`
	Name      string     `json:"name"`
	RootHash  *hash.H256 `json:"rootHash"`
	Segments  []Segment  `json:"segments"`
	pathMgr   *pathMgr   `json:"-"`
}

type pathMgr struct {
	homeDir string
}

func (t *pathMgr) getSegmentFilePath(index int) string {
	return filepath.Join(t.homeDir, fmt.Sprintf("seg-%d", index))
}

func (t *pathMgr) getFragDirPathOnSegment(index int) string {
	return filepath.Join(t.homeDir, fmt.Sprintf("seg-%d_frags", index))
}

func (t *pathMgr) getFragFilePath(index, belongSegmentIndex int) string {
	return filepath.Join(t.getFragDirPathOnSegment(belongSegmentIndex), fmt.Sprintf("frag-%d", index))
}

func (t *pathMgr) getSegmentFilePaths(segs []Segment) []string {
	paths := make([]string, len(segs))
	for i, seg := range segs {
		paths[i] = t.getSegmentFilePath(seg.Index)
	}
	return paths
}

func doSegmentsWrite[W io.Writer](data io.Reader, dst []W, size int64) error {
	if size == 0 {
		return errors.New("size must not be zero")
	}
	if len(dst) == 0 {
		return errors.New("dst must not be empty")
	}
	segCount := size / SEGMENT_SIZE
	if size%int64(SEGMENT_SIZE) != 0 {
		paddingSize := SEGMENT_SIZE - (size - segCount*SEGMENT_SIZE)
		data = io.MultiReader(data, io.LimitReader(zeroPaddingReader{}, paddingSize))
		segCount++
	}
	if int(segCount) != len(dst) {
		return errors.New("segmuent dst count not match")
	}

	// Split into equal-length segment.
	for i := range dst {
		n, err := io.CopyN(dst[i], data, SEGMENT_SIZE)
		if err != io.EOF && err != nil {
			return err
		}
		if n != SEGMENT_SIZE {
			return errors.New("write short data")
		}
	}

	return nil
}

type zeroPaddingReader struct{}

var _ io.Reader = &zeroPaddingReader{}

func (t zeroPaddingReader) Read(p []byte) (n int, err error) {
	n = len(p)
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	return n, nil
}

func splitStreamToSegments(inputStream io.Reader, size int64, pathMgr *pathMgr) ([]Segment, error) {
	segmentCount := int(size / SEGMENT_SIZE)
	if size%int64(SEGMENT_SIZE) != 0 {
		segmentCount++
	}
	segmentFiles := make([]*os.File, segmentCount)
	for i := 0; i < segmentCount; i++ {
		f, err := os.Create(pathMgr.getSegmentFilePath(i))
		if err != nil {
			return nil, err
		}
		segmentFiles[i] = f
	}
	if err := doSegmentsWrite(inputStream, segmentFiles, size); err != nil {
		return nil, err
	}
	segments := make([]Segment, segmentCount)
	for i, f := range segmentFiles {
		defer f.Close()
		_, err := f.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		segHash, err := hash.Calculate(f)
		if err != nil {
			return nil, err
		}
		segments[i] = Segment{
			Hash:  segHash,
			Index: i,
		}
	}
	return segments, nil
}

func shardSegmentToFrags(segmentFile *os.File, belongSegmentIndex int, pathMgr *pathMgr) ([]Fragment, error) {
	fragPaths := make([]string, 0, 16)
	builder := erasure.RsecEncodeByFile(segmentFile)
	builder.WithShards(cesspat.DataShards, cesspat.ParShards)
	builder.WithShardCreater(
		func(index int) (io.WriteCloser, error) {
			p := pathMgr.getFragFilePath(index, belongSegmentIndex)
			f, err := os.Create(p)
			if err != nil {
				return nil, err
			}
			fragPaths = append(fragPaths, p)
			return f, nil
		})
	builder.WithShardOpener(
		func(index int) (io.ReadCloser, int64, error) {
			f, err := os.Open(pathMgr.getFragFilePath(index, belongSegmentIndex))
			if err != nil {
				return nil, 0, err
			}
			fstat, err := f.Stat()
			if err != nil {
				return nil, 0, err
			}
			return f, fstat.Size(), nil
		})
	rsece, err := builder.Build()
	if err != nil {
		return nil, err
	}

	if err := rsece.Encode(); err != nil {
		return nil, err
	}

	frags := make([]Fragment, len(fragPaths))
	for i, fp := range fragPaths {
		f, err := os.Open(fp)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		fragHash, err := hash.Calculate(f)
		if err != nil {
			return nil, err
		}
		frags[i] = Fragment{
			Hash:               fragHash,
			Index:              i,
			belongSegmentIndex: belongSegmentIndex,
			pathMgr:            pathMgr,
		}
	}
	return frags, nil
}

func (t *FileSegmentMeta) ExtractFrags() []Fragment {
	frags := make([]Fragment, 0, len(t.Segments)*FRAG_SHARD_SIZE)
	for _, seg := range t.Segments {
		frags = append(frags, seg.Frags...)
	}
	return frags
}

func (t *FileSegmentMeta) ToFragSeg2dArray() [][]Fragment {
	segSize := len(t.Segments)
	a2dArray := make([][]Fragment, FRAG_SHARD_SIZE)
	for i := 0; i < FRAG_SHARD_SIZE; i++ {
		a2dArray[i] = make([]Fragment, segSize)
		for j, seg := range t.Segments {
			for n, frag := range seg.Frags {
				if i == n {
					a2dArray[i][j] = frag
					break
				}
			}
		}
	}
	return a2dArray
}

func (t *FileSegmentMeta) ChangeHomeDir(dir string) string {
	old := t.pathMgr.homeDir
	t.pathMgr.homeDir = dir
	return old
}

func (t *FileSegmentMeta) HomeDir() string { return t.pathMgr.homeDir }

func (t *Fragment) FilePath() string {
	return t.pathMgr.getFragFilePath(t.Index, t.belongSegmentIndex)
}

func CreateByStream(inputStream io.Reader, size int64, outputDir string) (*FileSegmentMeta, error) {
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		err = os.Mkdir(outputDir, 0755)
		if err != nil {
			return nil, err
		}
	}
	pathMgr := pathMgr{homeDir: outputDir}
	segs, err := splitStreamToSegments(inputStream, size, &pathMgr)
	if err != nil {
		return nil, err
	}
	merkletree, err := merklet.NewFromFilePaths(pathMgr.getSegmentFilePaths(segs))
	if err != nil {
		return nil, err
	}

	for i, seg := range segs {
		segFile, err := os.Open(pathMgr.getSegmentFilePath(seg.Index))
		if err != nil {
			return nil, err
		}
		fragsDir := pathMgr.getFragDirPathOnSegment(seg.Index)
		if _, err := os.Stat(fragsDir); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(fragsDir, os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
		frags, err := shardSegmentToFrags(segFile, seg.Index, &pathMgr)
		if err != nil {
			return nil, err
		}
		segs[i].Frags = frags
	}

	result := &FileSegmentMeta{
		InputSize: size,
		RootHash:  (*hash.H256)(merkletree.MerkleRoot()),
		Segments:  segs,
		pathMgr:   &pathMgr,
	}

	return result, err
}

func CreateByFile(srcFile *os.File, outputDir string) (*FileSegmentMeta, error) {
	fstat, err := srcFile.Stat()
	if err != nil {
		return nil, err
	}
	if fstat.IsDir() {
		return nil, errors.New("not a file")
	}
	return CreateByStream(srcFile, fstat.Size(), outputDir)
}

func CreateByFilePath(srcFilePath string, outputDir string) (*FileSegmentMeta, error) {
	f, err := os.Open(srcFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return CreateByFile(f, outputDir)
}
