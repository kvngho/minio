// Copyright (c) 2015-2025 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
)

func TestTierLZ4RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "json-like data",
			data: []byte(`{"sensor":"temp","value":23.5,"ts":"2025-01-01T00:00:00Z"}` + strings.Repeat(`{"sensor":"temp","value":23.5,"ts":"2025-01-01T00:00:00Z"}`, 100)),
		},
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "small data",
			data: []byte("hello"),
		},
	}

	// Add a larger random data test
	randomData := make([]byte, 1<<20) // 1MB
	if _, err := rand.Read(randomData); err != nil {
		t.Fatal(err)
	}
	tests = append(tests, struct {
		name string
		data []byte
	}{name: "1MB random data", data: randomData})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Compress
			var compressed bytes.Buffer
			lzw := lz4.NewWriter(&compressed)
			if _, err := lzw.Write(tt.data); err != nil {
				t.Fatalf("lz4 write: %v", err)
			}
			if err := lzw.Close(); err != nil {
				t.Fatalf("lz4 close: %v", err)
			}

			// Verify compression produced smaller output for compressible data
			if tt.name == "json-like data" {
				ratio := float64(compressed.Len()) / float64(len(tt.data))
				t.Logf("compression ratio: %.2f (compressed %d -> %d)", ratio, len(tt.data), compressed.Len())
				if ratio > 0.5 {
					t.Errorf("expected >50%% compression for JSON data, got ratio %.2f", ratio)
				}
			}

			// Decompress
			lzr := lz4.NewReader(&compressed)
			decompressed, err := io.ReadAll(lzr)
			if err != nil {
				t.Fatalf("lz4 decompress: %v", err)
			}

			if !bytes.Equal(tt.data, decompressed) {
				t.Errorf("round-trip failed: original %d bytes, got %d bytes", len(tt.data), len(decompressed))
			}
		})
	}
}

func TestTierLZ4StreamingRoundTrip(t *testing.T) {
	// Simulate the actual tiering flow: pipe writer -> lz4 writer -> pipe reader -> lz4 reader
	original := []byte(strings.Repeat(`{"key":"value","data":"some repeated json content for testing"}`, 500))

	// Compression side (simulates TransitionObject)
	pr, pw := io.Pipe()
	go func() {
		lzw := lz4.NewWriter(pw)
		if _, err := lzw.Write(original); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.CloseWithError(lzw.Close())
	}()

	// Decompression side (simulates getTransitionedObjectReader)
	lzr := lz4.NewReader(pr)
	decompressed, err := io.ReadAll(lzr)
	if err != nil {
		t.Fatalf("streaming decompress: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Fatalf("streaming round-trip failed: original %d bytes, got %d bytes", len(original), len(decompressed))
	}
}

func TestTierLZ4DecompressWithOffset(t *testing.T) {
	// Test that we can decompress and then seek to an offset (for range reads)
	original := []byte(strings.Repeat("abcdefghij", 1000)) // 10KB

	// Compress
	var compressed bytes.Buffer
	lzw := lz4.NewWriter(&compressed)
	if _, err := lzw.Write(original); err != nil {
		t.Fatal(err)
	}
	if err := lzw.Close(); err != nil {
		t.Fatal(err)
	}

	// Decompress with offset (simulate range read)
	off := int64(100)
	length := int64(500)

	lzr := lz4.NewReader(bytes.NewReader(compressed.Bytes()))

	// Skip to offset
	if _, err := io.CopyN(io.Discard, lzr, off); err != nil {
		t.Fatalf("skip to offset: %v", err)
	}

	// Read limited range
	rangeData, err := io.ReadAll(io.LimitReader(lzr, length))
	if err != nil {
		t.Fatalf("read range: %v", err)
	}

	expected := original[off : off+length]
	if !bytes.Equal(expected, rangeData) {
		t.Fatalf("range read mismatch: expected %d bytes, got %d bytes", len(expected), len(rangeData))
	}
}
