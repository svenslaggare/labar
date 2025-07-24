# Labar file reference
Each operation creates its own layer. The supported operations are now specified.

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