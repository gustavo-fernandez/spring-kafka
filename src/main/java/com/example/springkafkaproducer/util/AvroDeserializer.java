package com.example.springkafkaproducer.util;

import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

  private final Class<SpecificRecordBase> clazz;

  public AvroDeserializer(Class<SpecificRecordBase> clazz) {
    this.clazz = clazz;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T deserialize(String topic, byte[] data) {
    try {
      T result = null;

      if (data != null) {
        log.debug("data='{}'", DatatypeConverter.printHexBinary(data));

        DatumReader<GenericRecord> datumReader =
          new SpecificDatumReader<>(clazz.newInstance().getSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

        result = (T) datumReader.read(null, decoder);
        log.debug("deserialized data='{}'", result);
      }
      return result;
    } catch (Exception ex) {
      throw new SerializationException(
        "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
    }
  }
}
