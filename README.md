# stuffedio

Go log implementation using Consistent-Overhead Word Stuffing
(self-synchronizing journal). Based on the excellent writeup by Paul Khuong:
https://www.pvk.ca/Blog/2021/01/11/stuff-your-logs/.

The `stuffedio` package is simple: it implements COWS/COBS as described in the
above article, allowing you to wrap a writer and write stuffed log entries, or
wrap a reader and decode them, skipping corruption as needed, or as your use
case allows.

See examples in the code documentation for basic use.
