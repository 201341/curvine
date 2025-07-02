// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.curvine;

import io.curvine.exception.CurvineException;
import org.apache.hadoop.fs.Syncable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class CurvineOutputStream extends OutputStream implements Syncable {
    private long nativeHandle;
    private volatile byte[] oneByte;
    private final CurvineFsMount libFs;
    private boolean closed;

    private ByteBuffer buffer;

    private long pos = 0;

    public CurvineOutputStream(CurvineFsMount libFs, long nativeHandle, long pos, int bufferSize) {
        this.libFs = libFs;
        this.nativeHandle = nativeHandle;
        this.oneByte = new byte[1];
        this.buffer = CurvineNative.createBuffer(bufferSize);
        this.pos = pos;
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new CurvineException("stream has been closed!");
        }
    }

    @Override
    public void write(int b) throws IOException {
        checkClosed();
        oneByte[0] = (byte) b;
        write(oneByte, 0, oneByte.length);
    }

    @Override
    public void write(@Nonnull byte[] buf) throws IOException {
        checkClosed();
        write(buf, 0, buf.length);
    }

    @Override
    public void write(@Nonnull byte[] buf, int offset, int length) throws IOException {
        checkClosed();
        if (length == 0) {
            return;
        }

        if (offset > buf.length || offset + length > buf.length) {
            throw new IndexOutOfBoundsException("offset is out of buffer length.");
        }

        while (length > 0) {
            if (!buffer.hasRemaining()) {
               flushBuffer();
            }

            int writeLen = Math.min(length, buffer.remaining());
            buffer.put(buf, offset, writeLen);
            offset += writeLen;
            length -= writeLen;
            pos += writeLen;
        }
    }


    private void flushBuffer() throws IOException {
        if (buffer.position() > 0) {
            libFs.write(nativeHandle, buffer);
            buffer.clear();
        }
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
        libFs.flush(nativeHandle);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            flushBuffer();
            libFs.closeWriter(nativeHandle);
        } finally {
            nativeHandle = 0;
            buffer = null;
            closed = true;
            this.oneByte = null;
        }
    }

    public long pos() {
        return pos;
    }

    @Override
    public void hflush() throws IOException {
        flush();
    }

    @Override
    public void hsync() throws IOException {
        // pass
    }
}
