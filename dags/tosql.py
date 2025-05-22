import json

original = {
  "Directory": "ech",
  "fileName": "ech_2007.csv",
  "columnDelimiter": ",",
  "compressionCodec": "bzip2",
  "compressionLevel": "Optimal",
  "escapeChar": "\\",
  "quoteChar": "\"",
  "firstRowAsHeader": True
}

sql_string = "'" + json.dumps(original).replace("\\", "\\\\").replace("'", "''") + "'"
print(sql_string)
