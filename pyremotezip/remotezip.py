import urllib.request
import zlib

from urllib.error import HTTPError
from struct import unpack, unpack_from


class RemoteZip(object):
    """
    This class extracts single files from a remote ZIP file by using HTTP ranged requests
    """
    def __init__(self, zipURI):
        """
        zipURI should be an HTTP URL hosted on a server that supports ranged requests.
        The init function will determine if the file exists and raise a urllib2 exception if not.
        """
        self.zipURI = zipURI
        self.tableOfContents = None

    def _get_filesize(self):
        headRequest = urllib.request.Request(self.zipURI)
        headRequest.get_method = lambda: 'HEAD'

        response = urllib.request.urlopen(headRequest)
        return int(response.getheader('Content-Length'))

    def _request_range(self, uri, start, end):
        request = urllib.request.Request(self.zipURI)
        request.headers['Range'] = "bytes=%s-%s" % (start, end)
        handle = urllib.request.urlopen(request)

        # make sure the response is ranged
        return_range = handle.headers.get('Content-Range')
        if not return_range.startswith("bytes %d-%d/" % (start, end)):
            raise Exception("Ranged requests are not supported for this URI")

        return handle

    @staticmethod
    def __dos_date_to_date_tuple(date, time):
        day = date & 0b11111
        month = (date >> 5) & 0b1111
        year = 1980 + (date >> 9)
        second = (time & 0b11111) << 1
        minute = (time >> 5) & 0b111111
        hour = time >> 11
        return (year, month, day, hour, minute, second)

    def getTableOfContents(self):
        """
        This function populates the internal tableOfContents list with the contents
        of the zip file TOC. If the server does not support ranged requests, this will raise
        and exception. It will also throw an exception if the TOC cannot be found.
        """

        filesize = self._get_filesize()
        bytes_start = filesize - (65536)
        bytes_end = filesize - 1

        # now request bytes from that size minus a 64kb max zip directory length
        handle = self._request_range(self.zipURI, bytes_start, bytes_end)
        raw_bytes = handle.read()

        # now find the end-of-directory: 06054b50
        # we're on little endian maybe
        directory_end = raw_bytes.find(b"\x50\x4b\x05\x06")
        if directory_end < 0:
            raise Exception("Could not find end of directory")

        directory_size, directory_start = unpack_from("II", raw_bytes[directory_end+12:])
        if directory_start < bytes_start:
            handle = self._request_range(self.zipURI, directory_start, bytes_start-1)
            raw_bytes = handle.read() + raw_bytes
            bytes_start = directory_start

        # find the data in the raw_bytes
        current_start = directory_start - bytes_start
        filestart = 0
        compressedsize = 0
        tableOfContents = []

        try:
            while True:
                # get file name size (n), extra len (m) and comm len (k)
                zip_n = unpack("H", raw_bytes[current_start + 28: current_start + 28 + 2])[0]
                zip_m = unpack("H", raw_bytes[current_start + 30: current_start + 30 + 2])[0]
                zip_k = unpack("H", raw_bytes[current_start + 32: current_start + 32 + 2])[0]

                filename = raw_bytes[current_start + 46: current_start + 46 + zip_n]

                # check if this is the index file
                filestart = unpack("I", raw_bytes[current_start + 42: current_start + 42 + 4])[0]
                flags = unpack("H", raw_bytes[current_start + 8: current_start + 8 + 2])[0]
                compressionmethod = unpack("H", raw_bytes[current_start + 10: current_start + 10 + 2])[0]
                mtime = unpack("H", raw_bytes[current_start + 12: current_start + 12 + 2])[0]
                mdate = unpack("H", raw_bytes[current_start + 14: current_start + 14 + 2])[0]
                crc32 = unpack("I", raw_bytes[current_start + 16: current_start + 16 + 4])[0]
                compressedsize = unpack("I", raw_bytes[current_start + 20: current_start + 20 + 4])[0]
                uncompressedsize = unpack("I", raw_bytes[current_start + 24: current_start + 24 + 4])[0]
                tableItem = {
                    'filename': filename,
                    'compressedsize': compressedsize,
                    'uncompressedsize': uncompressedsize,
                    'filestart': filestart,
                    'flags': flags,
                    'compressionmethod': compressionmethod,
                    'crc32': crc32,
                    'modifieddate': self.__dos_date_to_date_tuple(mdate, mtime),
                }
                tableOfContents.append(tableItem)

                # not this file, move along
                current_start = current_start + 46 + zip_n + zip_m + zip_k
        except:
            pass

        self.tableOfContents = tableOfContents
        return tableOfContents

    def extractFile(self, filename):
        """
        This function will extract a single file from the remote zip without downloading
        the entire zip file. The filename argument should match whatever is in the 'filename'
        key of the tableOfContents.
        """
        files = [x for x in self.tableOfContents if x['filename'] == filename]
        if len(files) == 0:
            raise FileNotFoundException()

        fileRecord = files[0]

        # got here? need to fetch the file size
        metaheadroom = 1024  # should be enough
        start = fileRecord['filestart']
        end = fileRecord['filestart'] + fileRecord['compressedsize'] + metaheadroom
        handle = self._request_range(self.zipURI, start, end)
        filedata = handle.read()

        # find start of raw file data
        zip_n = unpack("H", filedata[26:28])[0]
        zip_m = unpack("H", filedata[28:30])[0]

        # check compressed size
        has_data_descriptor = bool(unpack("H", filedata[6:8])[0] & 8)
        comp_size = unpack("I", filedata[18:22])[0]
        if comp_size == 0 and has_data_descriptor:
            # assume compressed size in the Central Directory is correct
            comp_size = fileRecord['compressedsize']
        elif comp_size != fileRecord['compressedsize']:
            raise Exception("Something went wrong. Directory and file header disagree of compressed file size")

        raw_zip_data = filedata[30 + zip_n + zip_m: 30 + zip_n + zip_m + comp_size]
        uncompressed_data = ""
        
        # can't decompress if stored without compression
        compression_method = unpack("H", filedata[8:10])[0]
        if compression_method == 0:
          return raw_zip_data

        dec = zlib.decompressobj(-zlib.MAX_WBITS)
        return dec.decompress(raw_zip_data)


class FileNotFoundException(Exception):
    pass