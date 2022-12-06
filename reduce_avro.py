import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

schema_file = input("Schema file: ")
schema = avro.schema.parse(open(schema_file, "rb").read())

big_avro = input("Original avro file: ")
reader = DataFileReader(open(big_avro, "rb"), DatumReader())

writer = DataFileWriter(open(big_avro.replace(".avro", "_small.avro"), "wb"), DatumWriter(), schema)

count=0
for row in reader:
    writer.append(row)
    count+=1
    if count > 0:
        break

writer.close()
reader.close()
print(count)