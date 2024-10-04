package org.joksin.parquetconverter;

import java.io.File;
import java.util.List;

public class Main {
  public static void main(String[] args) throws Exception {
    deleteParquetFiles();

    var parquetConverter = new ParquetConverter();
    parquetConverter.convertCsvToParquet(
        List.of("data/test_data_1.csv", "data/test_data_2.csv"), "data/test_date.parquet");
  }

  private static void deleteParquetFiles() {
    var parquetFile = new File("data/test_date.parquet");
    if (parquetFile.exists()) {
      parquetFile.delete();
      System.out.println("Parquet file is deleted");
    } else {
      System.out.println("Parquet file does not exist");
    }

    var crcFile = new File("data/.test_date.parquet.crc");
    if (crcFile.exists()) {
      crcFile.delete();
      System.out.println("CRC file is deleted");
    } else {
      System.out.println("CRC file does not exist");
    }
  }
}
