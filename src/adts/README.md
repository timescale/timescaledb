# A Collection of ADTs

This directory contains a collection of Abstract Data Types.
These ADTS are containers that can store data of any other type.
These ADTs use macros as oppossed to void pointers for performance
reasons, as well as for better type safety. This approach to
ADTs follows Postgres convention (see simplehash).

## Simplehash

This is a hash table implementation. Copied over from Postgres (where it's
available after PG 11).

## Vector

A dynamic vector implementation that can store any type. It handles
growing/shrinking the memory for you.

## Bit Array

A dynamic vector to store bits. The API allows appending and iterating
an arbitrary amount of bits. It stores the bits in a vector of uint64
and has methods to serialize/deserialize.
