# -*- coding: utf8 -*-
import socket, os, struct, threading, sys, cPickle
# 使用5个线程发送
from multiprocessing import Process, Queue

from main import q

ThreadCount = 1
sendQueue = Queue()
remoteHost = ""
localPath = ""
targetPath = ""

def searchPath(targetPath, localPath, remoteHost):
    fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd.connect((remoteHost, 20011))
    while True:
        try:
            path = sendQueue.get()
        except:
            print "Thread", threading.currentThread().name, "exited"
            return
        print threading.currentThread().name, ":", "Sending", path
        target = os.path.join(targetPath, path[len(localPath):].lstrip("/"))
        fd.send(path)
        nameok = fd.recv(64)
        if nameok == 'name ok':
            fd.send(file(path, "rb").read())
            print "finish",path
            fd.close()
        # packet = {"path" : target, "data" : file(path, "rb").read() }
        # data = cPickle.dumps(packet)
        # fd.send(struct.pack("I", len(data)))
        # fd.send(data)
        else:
            print "something error"
        break


def sendfile_client(localPath, targetPath, remoteHost):
    func_name = sys._getframe().f_code.co_name
    processpool=[]
    if os.path.exists(localPath):
        for parent,dirs,files in os.walk(localPath):
            for f in files:
                sendQueue.put(os.path.join(parent, f))
        # print "Start to transfer container files, ", sendQueue.qsize(), "files!"
        size = sendQueue.qsize()
        print size
        for i in range (0,size):
            p = Process(target=searchPath, args=(targetPath, localPath, remoteHost))
            p.daemon = True
            processpool.append(p)
            p.start()
        for item in processpool:
            item.join()
        q.put((True, func_name))
    else:
        print ("File not found:", localPath)
        q.put((False, func_name))

if __name__ == "__main__":
    if len(sys.argv)!=4:
        print "Usage: senddir SourcePath TargetPath IP"
        exit()
    localPath, targetPath, remoteHost = sys.argv[1:]
    sendfile_client(localPath, targetPath, remoteHost)