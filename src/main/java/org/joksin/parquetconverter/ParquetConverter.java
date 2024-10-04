package org.joksin.parquetconverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

public class ParquetConverter {

  public void convertCsvToParquet(List<String> csvFilePaths, String parquetFilePath)
      throws Exception {

    var schemaString =
        """
            {
               "type":"record",
               "name":"CSVRecord",
               "fields":[
                  {
                     "name":"c1",
                     "type":"string"
                  },
                  {
                     "name":"c2",
                     "type":"string"
                  },
                  {
                     "name":"c3",
                     "type":"double"
                  },
                  {
                     "name":"c4",
                     "type":"int"
                  }
               ]
            }
            """;

    var schema = new Schema.Parser().parse(schemaString);

    try (var writer =
        AvroParquetWriter.<GenericRecord>builder(new Path(parquetFilePath))
            .withSchema(schema)
            .build()) {

      for (var csvFilePath : csvFilePaths) {
        System.out.println("Processing file " + csvFilePath);

        try (var reader = new BufferedReader(new FileReader(csvFilePath));
            var csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); ) {

          for (var record : csvParser) {
            var avroRecord = new GenericData.Record(schema);
            avroRecord.put("c1", record.get("c1"));
            avroRecord.put("c2", record.get("c2"));
            avroRecord.put("c3", Double.parseDouble(record.get("c3")));
            avroRecord.put("c4", Integer.parseInt(record.get("c4")));

            // Add more columns as needed
            writer.write(avroRecord);
          }
        }
      }
    }

    System.out.println("All files processed");
  }
}
