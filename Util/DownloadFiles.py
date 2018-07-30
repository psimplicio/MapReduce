# coding: utf-8

import io
import sys
import urllib.request as request

class Download():

    def __init__(self):
        self.BUFF_SIZE = 1024

    def download_length(self, response, output, length):
        times = length // self.BUFF_SIZE
        if length % self.BUFF_SIZE > 0:
            times += 1
        for time in range(times):
            output.write(response.read(self.BUFF_SIZE))
            print("Downloaded %d" % (((time * self.BUFF_SIZE)/length)*100))
        return 'Done!'
    
    def download(self, response, output):
        total_downloaded = 0
        while True:
            data = response.read(self.BUFF_SIZE)
            total_downloaded += len(data)
            if not data:
                break
            out_file.write(data)
            print('Downloaded {bytes}'.format(bytes=total_downloaded))
        return 'Done!'

if __name__ == '__main__':

    response = request.urlopen(sys.argv[1])
    out_file = io.FileIO(sys.argv[2], mode="w")
    content_length = response.getheader('Content-Length')

    download = Download()

    try:
        if content_length:
            length = int(content_length)
            print(download.download_length(response, out_file, length))
        else:
            print(download.download(response, out_file))
    except Exception as e:
        print("Erro durante o download do arquivo {}".format(sys.argv[1]))
    finally:
        out_file.close()
        response.close()