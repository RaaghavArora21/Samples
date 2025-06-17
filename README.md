import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;

public static byte[] writeParquetToBytes(GenericRecord record, Schema schema) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    OutputFile outputFile = new OutputFile() {
        public PositionOutputStream create(long blockSizeHint) { return new PositionOutputStream() {
            public void write(int b) { out.write(b); }
            public void write(byte[] b, int off, int len) { out.write(b, off, len); }
            public long getPos() { return out.size(); }
            public void flush() {}
            public void close() {}
        }; }
        public PositionOutputStream createOrOverwrite(long blockSizeHint) { return create(blockSizeHint); }
        public boolean supportsBlockSize() { return false; }
        public long defaultBlockSize() { return 0; }
    };

    try (AvroParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {
        writer.write(record);
    }
    return out.toByteArray();
}
