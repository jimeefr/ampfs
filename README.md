# ampfs : AMP fuse filesystem

This creates a userland filesystem which gives access to AMP.

Amiga Music Preservation : https://amp.dascene.net

## Howto

```shell
$ virtualenv .
$ . bin/activate
(ampfs) $ pip install -r requirements.txt
(ampfs) $ mkdir /some/path
(ampfs) $ ./ampfs.py /some/path
```
Now you can browse `/some/path`...
