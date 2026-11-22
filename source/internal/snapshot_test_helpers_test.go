package internal

import (
	"os"
	"path/filepath"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// newFilesystemTestSink wires the real filesystem sink at dir so readBase
// exercises the same ListFiles/ReadFile path used in production.
func newFilesystemTestSink(dir string) (ports.Sink, error) {
	return filesystem.New(dir)
}

func writeTestParquet(fullPath string, rows []writer.Row, columns []manifest.Column) error {
	return writer.WriteBaseParquet(fullPath, rows, columns)
}

func writeTestFile(fullPath string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(fullPath, data, 0o644)
}
