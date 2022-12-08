import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import os

big_avro = input("Original avro file: ")

reader = DataFileReader(open(big_avro,"rb"), DatumReader())
schema = reader.meta
schema = schema['avro.schema'].decode("utf-8")
schema_file = open("tempschema.avsc", "w")
n = schema_file.write(schema)
schema_file.close()
schema = avro.schema.parse(open("tempschema.avsc", "rb").read())

num_rows = int(input("Number of rows: "))

writer = DataFileWriter(open(big_avro.replace(".avro", "_" + str(num_rows) + ".avro"), "wb"), DatumWriter(), schema)

count=0
for row in reader:
    if count >= num_rows:
        break
    writer.append(row)
    count+=1

writer.close()
reader.close()
os.remove("tempschema.avsc")