# Labar - LAyer Based ARchive

Labar brings a docker inspired way of managing static files. In Labar, an archive is called an _image_, and is a linked list of _layers_. Each layer is a collection of files, directories and references to other images. Images can be tagged to create new identifies for them, and pushed and pulled to registries.

To make the content of an image available to the computer, it can be unpacked. This is made using  links such that the data is not duplicated.

This approach makes it easy to do incremental changes to the archives and re-use of large files without needing to think about data duplication on the consumer side. It will just pull the layers, and if different images uses the same layers, they are not duplicated.

## Building images
Run the `labar build` command to build an image. Images are defined in _labarfiles_ similar to Docker files.

The following definition would create an image that copies a file as `file1.txt`.
```
COPY testdata/rawdata/file1.txt file1.txt
```

## Unpacking images
To unpack the image (make the content available), use the `labar unpack` command. This will unpack the folder structure into a new folder, but the actual files are linked into new directory, leading to no extra space used.