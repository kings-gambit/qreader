# qreader.go #

_Building / installing_

There are no external dependencies for this file, so a simple `go build` will work.

In the source code, change the values of `Unzipper` to the gzip unreader you want to use (gunzip vs gzcat vs unpigz...)

Then set `ParserPool` and `ReducerPool` to the number of goroutines you would like to have.

_Usage_

coming soon...
