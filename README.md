# Labar - LAyer Based ARchive

Labar tries to bring a docker inspired way of managing archives of files. An archive is called an image in Labar and is a linked list of immutable layers. Each layer is a collection of files, directories and references to other images.

To make the content of an image available to the computer, it can be unpacked. This is made using symbolic links such that the data is not duplicated.

This approach makes it easy to do incremental changes to the archives and re-use it without worrying about data duplication.

Advantages compared to traditional archive files (e.g. zip):
* Very easy implementation to handle incremental changes/re-use.
* No duplication of data when having both archive and unpacked data on computer.
* Fits well into a tag based (like Docker) deployment system.

Disadvantages:
* No compression of archive.
* Not a standard format.