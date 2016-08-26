# Stream Real-time Sink


Description
-----------
Real-time sink that outputs to a specified CDAP Stream.


Use Case
--------
This sink is used when you want to write to a stream in real-time. For example, you
may want to read data from Kafka and write it to a stream.


Properties
----------
**name:** The name of the stream to output to. Must be a valid stream name. The stream
will be created if it does not exist.

**body.field:** Name of the field in the record that contains the data to be written to
the specified stream. The data could be in binary format as a byte array or a ByteBuffer.
It can also be a String. If unspecified, the 'body' key is used.

**headers.field:** Name of the field in the record that contains headers. Headers are
presumed to be a map of string to string.


Example
-------
This example will write to a stream named 'purchases'. Each record it receives will be written
as a single stream event. The stream event body will be equal to the value of the 'message' field
from the input record. No headers will be written in this example because the 'headers.field'
property is not set:

    {
        "name": "Stream",
        "type": "realtimesink",
        "properties": {
            "name": "purchases",
            "body.field": "message"
        }
    }
