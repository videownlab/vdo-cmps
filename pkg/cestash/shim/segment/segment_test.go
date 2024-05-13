package segment

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func generateFile(fileName string, numOfNums int64) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for i := int64(1); i <= numOfNums; i++ {
		err := writer.WriteByte(byte(i & 0xff))
		if err != nil {
			return err
		}
	}
	return writer.Flush()
}

func TestCreateFileSegmentMeta(t *testing.T) {
	dir := ".test-tmp"
	require := require.New(t)
	require.NoError(os.MkdirAll(dir, 0744))
	srcFile := filepath.Join(dir, "data.dat")
	require.NoError(generateFile(srcFile, 64<<20)) //16 MiB
	defer os.Remove(srcFile)
	t.Log("file is ready")
	fsm, err := CreateByFilePath(srcFile, dir)
	require.NoError(err)
	t.Log(fsm)
	require.NotEmpty(fsm.Segments)
	require.NotEmpty(fsm.Segments[0].Frags)
}
