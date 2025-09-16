# Labar - LAyer Based ARchive

Labar brings a docker inspired way of managing static files. In Labar, an archive is called an _image_, and is a linked list of _layers_. Each layer is a collection of files, directories and references to other images. Images can be tagged to create new identifies for them, and pushed and pulled to registries.

To make the content of an image available to the computer, it can be unpacked. This is made using  links such that the data is not duplicated.

This approach makes it easy to do incremental changes to the archives and re-use of large files without needing to think about data duplication on the consumer side. It will just pull the layers, and if different images uses the same layers, they are not duplicated.

## Building

* Requires cargo (https://rustup.rs/).
* Build with: `cargo build --release`
* Build output in `target/release/labar`

### Debian package
A debian package can be built using the `build_deb.sh` command. This will also include bash auto-completions.

## Building images
Run the `labar build` command to build an image. Images are defined in _labarfiles_ similar to Docker files, see this [file](./LABARFILE_REFERENCE.md) for a definition of the format. 

The following definition would create an image that copies a file on the host into the image as `file1.txt`.
```
COPY testdata/rawdata/file1.txt file1.txt
```

## Unpacking images
To unpack the image (to make the content available), use the `labar unpack` command. This will unpack the folder structure into a new folder, but the actual files are linked into new directory, leading to no extra space used.

## Registry
To distribute images, Labar uses an HTTP based registry. This can be started using `labar registry run` command.

To configure the registry, use the sample TOML configuration file below:
```toml
data_path = "<HOME>/.labar-registry"
address = "0.0.0.0:3000"
initial_users = [
    ["guest", "84983c60f7daadc1cb8698621f802c0d9f9a3c3c295c810748fb048115c186ec", ["List", "Download", "Upload", "Delete"]]
]
```

The specified password is a SHA256 hash of the actual password (`guest` in the example).