# -*- coding: utf8 -*-
import socket, cPickle, os, threading, struct
from multiprocessing import Process


def receive_process(clientfd):
    clientReader = clientfd.makefile("rb")
    filename = clientfd.recv(1024)
    print filename
    parent = os.path.dirname(filename)
    if not os.path.exists(parent):
        try:
            os.makedirs(parent)
        except OSError as e:
            print e.message
            pass
    if not os.path.exists(filename):
        createFile = file(filename, 'w')
    else:
        createFile = file(filename, 'w')
    clientfd.send('name ok')
    while True:
        data = clientfd.recv(8192)
        if not data:
            break
        createFile.write(data)
    print "Received file", filename
    createFile.close()
    # while True:
    #     # 接收数据包的大小
    #     data = clientReader.read(4)
    #     if len(data) != 4:
    #         break
    #     dataLength = struct.unpack("I", data)[0]
    #     data = clientReader.read(dataLength)
    #     packet = cPickle.loads(data)
    #     path = packet["path"]
    #     # 递归创建目录
    #     parent = os.path.dirname(path)
    #     if not os.path.exists(parent):
    #         try:
    #             os.makedirs(parent)
    #         except OSError as e:
    #             print e.message
    #             file(path, "wb").write(packet["data"])
    #             print "Received file", path
    #             clientfd.send('\xff')
    #             continue
    #     file(path, "wb").write(packet["data"])


def server_process():
    fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # 设置重用标记，这样重启程序的时候不会提示端口被占用。
    fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    fd.bind(("", 20012))
    fd.listen(5)
    while True:
        # 等待客户端连接
        clientfd, addr = fd.accept()
        thread = Process(target=receive_process, args=(clientfd,))
        # 设置Daemon属性可以让server结束，则所有子线程必须也退出
        thread.daemon = True
        thread.start()


if __name__ == '__main__':
    try:
        server_process()
    except KeyboardInterrupt:
        exit()