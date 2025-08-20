# Labar file reference
Images are defined in the _labarfile_ format. See [labar/testdata/parsing/success ](./labar/testdata/parsing/success) folder for examples. Each operation (as defined below) creates its own _layer_.

## COPY
Copies a file from the build context into the image.

**Arguments**:

* writable (yes/no) - Makes the unpacked file writable. Default is no.
* link (soft/hard) - Use soft or hard links. Default is hard.

**Examples**:

* `COPY data/test1.txt test1.txt`
* `COPY --writable=yes data/test1.txt test1.txt`
* `COPY --link=soft data/test1.txt test1.txt`

## MKDIR
Creates a new directory in the image.

**Examples**:

* `MKDIR test`

## IMAGE
Merges the referred to image into the current image.

**Examples**:

* `IMAGE test:latest`

## BEGIN LAYER
Creates a new layer. All operations within the layer block will be considered to be one operation.

**Examples**:

```
BEGIN LAYER
    COPY data/test1.txt test1.txt
    MKDIR test
END
```