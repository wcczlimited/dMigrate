import Queue
import os
import shutil
import socket
import struct
import threading
import string
import cPickle
from multiprocessing import Process
import multiprocessing
from time import sleep

import requests
import sys
import network_utils

q = multiprocessing.Queue()
sendQueue = multiprocessing.Queue()
sendMountQueue = multiprocessing.Queue()
afterFinishedQueue = Queue.Queue()

def searchImageOnRemoteHost(remotehost, containerid):
    docker_url= 'http://{0}:4433/images/json'.format(remotehost)
    r = requests.get(docker_url)
    if r.status_code != requests.codes.ok:
        print ("error return from remote Docker HTTP API")
        exit(1);

    local_url='http://127.0.0.1:4433/containers/{0}/json'.format(containerid)
    local_response = requests.get(local_url)
    if local_response.status_code != requests.codes.ok:
        print ("error return local from Docker HTTP API")
        exit(2);

    local_imageid = local_response.json()['Image']
    for item in r.json():
        if local_imageid == item["Id"]:
            return True
    return False


def getImageIDAndContainernameById(containerid):
    local_url='http://127.0.0.1:4433/containers/{0}/json'.format(containerid)
    local_response = requests.get(local_url)
    if local_response.status_code != requests.codes.ok:
        print ("error return local from Docker HTTP API")
        exit(2);

    local_imageid = local_response.json()['Config']['Image']
    local_containername=local_response.json()['Name'][1:]
    return local_imageid, local_containername


def createMirrorContainerAndCopyFilesOnRemoteHost(remoteHost, containerid, containername):
    func_name = sys._getframe().f_code.co_name
    result = searchImageOnRemoteHost(remoteHost, containerid)
    # There is no such image on remote host
    if result != True:
        print "not find image"
        # imagename, _ = getImageIDAndContainernameById(containerid)
        getLocalTarballImageAndImportToRemote(containerid, remoteHost)
        print "success to push image"
    remoteContainerID = createContainerOnRemoteHost(remoteHost, containerid, containername)
    renameContainerOnRemoteHost(remoteHost, remoteContainerID, containername)
    # make the change on the container to the remote
    # copy changed files when container is created and stopped
    getChangedFilesFromLocalContainerAndSendToRemoteContainer(containerid, remoteHost, remoteContainerID)
    q.put((True, func_name))
    q.put((remoteContainerID, "remotecontainerID"))
    print "create mirror container success"
    return True


def getChangesFromLocalContainer(containerid):
    localurl = 'http://127.0.0.1:4433/containers/{0}/changes'.format(containerid)
    response = requests.get(localurl)
    return response.json()


def makedirOnContainer(remoteHost, containerid, path):
    remoteurl = 'http://{0}:4433/containers/{1}/exec'.format(remoteHost,containerid)
    cmd = {"AttachStdin": False,
           "AttachStdout": False,
           "AttachStderr": False,
           "Detach" : True,
           "Tty": False,
           "Cmd": [
                "mkdir",
                "-p",
                path
           ]
    }
    response = requests.post(remoteurl, json=cmd)
    execid = response.json()["Id"]
    remoteurl = 'http://{0}:4433/exec/{1}/start'.format(remoteHost, execid)
    start = {
        "Detach" : True,
        "Tty" : False
    }
    response = requests.post(remoteurl, json=start)
    print "mkdir", path, response.content


def getChangedFilesFromLocalContainerAndSendToRemoteContainer(containerid, remoteHost, remoteContainerid):
    changes = getChangesFromLocalContainer(containerid)
    localurl = 'http://127.0.0.1:4433/containers/{0}/json'.format(containerid)
    local_response = requests.get(localurl)
    mounts = local_response.json()["Mounts"]
    disables = []
    for item in mounts:
        disables.append(item["Destination"])
    disables.append("/proc")
    disables.append("/sys")
    disables.append("/dev")
    disables.append("/tmp")
    success={}
    secondSend={}
    for item in changes:
        if item["Kind"] != 2:
            path = item["Path"]
            if path in disables:
                continue
            if '/tmp' in path:
                continue
            print "transfer",path
            localurl = 'http://127.0.0.1:4433/containers/{0}/archive?path={1}'.format(containerid, path)
            local_response = requests.get(localurl)
            archive = local_response.content
            # start to commit to remote container
            path = path[0:path.rfind("/")+1]
            remoteurl = 'http://{0}:4433/containers/{1}/archive?path={2}'.format(remoteHost, remoteContainerid, path)
            response = requests.put(remoteurl, data=archive)
            print path, response.content,"1st"
    for path, archive in secondSend.items():
         remoteurl = 'http://{0}:4433/containers/{1}/archive?path={2}'.format(remoteHost, remoteContainerid, path)
         response = requests.put(remoteurl, data=archive)
         if response.status_code == requests.codes.ok:
             print path, "2nd"
             del secondSend[path]
         else:
             afterFinishedQueue.put((path,archive))
             print path, response.content


def sendChangedFilesAfterFinish(remoteHost, remoteContainerid):
    while not afterFinishedQueue.empty():
         item = afterFinishedQueue.get()
         path = item[0]
         archive = item[1]
         makedirOnContainer(remoteHost,remoteContainerid,path)
         remoteurl = 'http://{0}:4433/containers/{1}/archive?path={2}'.format(remoteHost, remoteContainerid, path)
         response = requests.put(remoteurl, data=archive)
         if response.status_code == requests.codes.ok:
             print path, "3th"
         else:
             print path, response.content



def getLocalTarballImageAndImportToRemote(containerid, remoteHost):
    # Get the ImageId from localhost By containerID
    imageid, container_name = getImageIDAndContainernameById(containerid)

    # get repoTags from localhost by ImageID
    localurl='http://127.0.0.1:4433/images/{0}/json'.format(imageid)
    local_response = requests.get(localurl)
    if local_response.status_code != requests.codes.ok:
        print ("error return local from Docker HTTP API 2")
        exit(2);
    repoTags = local_response.json()['RepoTags']

    # Get the binary data of image by ImageID
    localurl = "http://127.0.0.1:4433/images/get?names={0}".format(imageid)
    local_response = requests.get(localurl)

    # Load the image to th remote host by the binary above
    remoteurl = "http://{0}:4433/images/load".format(remoteHost)
    requests.post(remoteurl, data=local_response.content)

    # tag the remote host image by the repo tags above
    for item in repoTags:
        items = string.split(item, ':')
        remoteurl = 'http://{0}:4433/images/{1}/tag?repo={2}&tag={3}'.format(remoteHost, imageid, items[0], items[1])
        requests.post(remoteurl)
    return


def getThePathContainerMounted(containerid):
    localurl = 'http://127.0.0.1:4433/containers/{0}/json'.format(containerid)
    response = requests.get(localurl)
    return response.json()['Mounts']


# restore a container on remote host
def restoreContainerOnRemoteHost(remoteHost,remotecontainerid,imagedir, logdir):
    remote_url = 'http://{0}:4433/containers/{1}/json'.format(remoteHost, remotecontainerid[0:6])
    response = requests.get(remote_url)
    # while response.json()["State"]["Status"] != "exited":
    #     print response.json()["State"]["Status"]
    #     remote_url = 'http://{0}:4433/containers/{1}/json'.format(remoteHost, remotecontainerid[0:6])
    #     response = requests.get(remote_url)
    remote_url = 'http://{0}:4433/containers/{1}/restore?force=1'.format(remoteHost, remotecontainerid[0:6])
    data = {'ImagesDirectory':imagedir, 'WorkDirectory':logdir}
    i = 0
    while i < 3:
        response = requests.post(remote_url, json=data)
        if response.status_code == requests.codes.no_content:
            color = "\x1B[%d;%d;%dm" % (1, 32, 49)
            print "Restore container success, id is", "\x1B[%s%s\x1B[0m" % (color, remotecontainerid[0:8])
            return True
        i += 1
    print "Restore container error, please check {0}".format(remotecontainerid)
    print response.content
    return False


# rename the container to the same name on the local host
def renameContainerOnRemoteHost(remoteHost,remotecontainerid, containername):
    remote_url = 'http://{0}:4433/containers/{1}/rename?name={2}'.format(remoteHost,remotecontainerid, containername)
    response = requests.post(remote_url)
    if response.status_code != requests.codes.no_content:
        print "rename container {0} to {1} error".format(remotecontainerid, containername), response.content
        exit(3)

    # Make sure rename success and return
    remote_url = 'http://{0}:4433/containers/{1}/json'.format(remoteHost, remotecontainerid)
    response = requests.get(remote_url)
    name = response.json()['Name'][1:]
    while name != containername:
        print "renaming"
        sleep(0.01)
        remote_url = 'http://{0}:4433/containers/{1}/json'.format(remoteHost, remotecontainerid)
        response = requests.get(remote_url)
        name = response.json()['Name'][1:]
    remote_url = 'http://{0}:4433/containers/{1}/kill'.format(remoteHost, remotecontainerid)
    requests.post(remote_url)
    while True:
        try:
            requests.post(remote_url, timeout=1)
        except requests.exceptions.Timeout:
            print "Timeout occurred"
            continue
        break
    return True


def deleteContainerOnRemoteHost(remoteHost, containerid):
    remote_url = 'http://{0}:4433/containers/{1}/kill'.format(remoteHost, containerid)
    while True:
        try:
            requests.post(remote_url, timeout=0.2)
        except requests.exceptions.Timeout:
            print "kill Timeout occurred"
            continue
        break
    remoteurl = 'http://{0}:4433/containers/{1}?v=1&force=1'.format(remoteHost, containerid)
    while True:
        try:
            requests.delete(remoteurl, timeout=0.2)
        except requests.exceptions.Timeout:
            print "delete Timeout occurred"
            continue
        break

def checkContainerExistsOnRemoteHost(remoteHost, container_name):
    remoteurl = 'http://{0}:4433/containers/json?all=1'.format(remoteHost)
    response = requests.get(remoteurl)
    data = response.json()
    for item in data:
        names = item["Names"]
        for name in names:
            temp = name[1:]
            if temp == container_name:
                color = "\x1B[%d;%d;%dm" % (1, 31, 49)
                print "Find container", "\x1B[%s%s\x1B[0m" % (color, temp), "exist on remote host, delete first"
                id = item["Id"]
                deleteContainerOnRemoteHost(remoteHost, id)
                break


# create specific image on remote host
def createContainerOnRemoteHost(remoteHost, containerid, containername):
    checkContainerExistsOnRemoteHost(remoteHost, containername)
    remote_url = 'http://{0}:4433/containers/create'.format(remoteHost)
    local_url = 'http://127.0.0.1:4433/containers/{0}/json'.format(containerid)
    local_response = requests.get(local_url)
    if local_response.status_code != requests.codes.ok:
        print ("error return local from Docker HTTP API")
        exit(2)
    # get the container made it as the json to create container on remote host
    request_json = local_response.json()
    # adjust Param in json
    request_json_replace = request_json['Config']
    request_json_replace.update({"Mounts": request_json['Mounts'],
                                 "NetworkDisabled": False,
                                 "MacAddress": request_json['NetworkSettings']['MacAddress'],
                                 "HostConfig": request_json['HostConfig']})
    network = {"EndpointsConfig": request_json["NetworkSettings"]["Networks"]}
    request_json_replace.update({"NetworkingConfig":network})
    #networkName,_ = request_json["NetworkSettings"]["Networks"].items()[0]
    #print "create network", networkName, "on remote", remoteHost
    #network_utils.createNetworkOnRemote(remoteHost, networkName)
    request_json_replace['Hostname'] = ""
    # request_json_replace['Cmd'] = ["/bin/true"]
    remote_response = requests.post(url=remote_url, json=request_json_replace)

    if remote_response.status_code != requests.codes.created:
        print "create container failed on Host {0}".format(remoteHost), remote_response.content
        exit(5)
    remote_url = 'http://{0}:4433/containers/{1}/start'.format(remoteHost, remote_response.json()["Id"])
    requests.post(remote_url)
    remoteid = remote_response.json()["Id"]
    return remoteid


# make a checkpoint on a local specific container
def localcheckpoint(containerid, imagedir, logdir, leaverunning, containername):
    func_name = sys._getframe().f_code.co_name
    if os.path.exists(imagedir):
        color = "\x1B[%d;%d;%dm" % (1, 31, 49)
        print "Find dir", "\x1B[%s%s\x1B[0m" % (color, imagedir), "exist, delete first"
        shutil.rmtree(imagedir)
    local_url='http://127.0.0.1:4433/containers/{0}/checkpoint'.format(containerid)
    r = requests.post(local_url, json={'ImagesDirectory':imagedir, 'WorkDirectory':logdir, 'LeaveRunning':leaverunning})
    if r.status_code != requests.codes.no_content and r.status_code != requests.codes.ok:
        print "checkpoint error", r.content
        exit(3)
    q.put((True, func_name))
    return True


# The barrier to get all the thread be done
class Barrier:
    def __init__(self, n):
        self.n = n
        self.count = 0
        self.mutex = threading.Semaphore(1)
        self.barrier = threading.Semaphore(0)

    def wait(self):
        self.mutex.acquire()
        self.count = self.count + 1
        self.mutex.release()
        if self.count == self.n: self.barrier.release()
        self.barrier.acquire()
        self.barrier.release()

# The client for transfering image files
ThreadCount = 6
b = Barrier(ThreadCount)
def searchPath(targetPath, localPath, remoteHost):
    fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd.connect((remoteHost, 20011))
    while True:
        try:
            path = sendQueue.get()
        except:
            #print "Thread", threading.currentThread().name, "exited"
            return
        fd.send(path)
        nameok = fd.recv(64)
        if nameok == 'name ok':
            fd.send(file(path, "rb").read())
            fd.close()
        else:
            print "something error"
        break


def sendfile_client(localPath, targetPath, remoteHost):
    from time import time
    start = time()
    func_name = sys._getframe().f_code.co_name
    processpool=[]
    if os.path.exists(localPath):
        for parent,dirs,files in os.walk(localPath):
            for f in files:
                sendQueue.put(os.path.join(parent, f))
        # print "Start to transfer container files, ", sendQueue.qsize(), "files!"
        size = sendQueue.qsize()
        for i in range(0,size):
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
    finish = time()
    print "send", (finish-start)


def searchMountPath(targetPath, localPath,remoteHost,first):
    fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd.connect((remoteHost, 20012))
    while True:
        try:
            path = sendMountQueue.get()
        except:
            # print "Thread", threading.currentThread().name, "exited"
            return
        fd.send(path)
        nameok = fd.recv(64)
        if nameok == 'name ok':
            if first:
                fd.send('')
            else:
                fd.send(file(path, "rb").read())
            fd.close()
        else:
            print "something error"
        break


def sendMountfile_client(localPath, targetPath, remoteHost, threadpool, first):
    if os.path.exists(localPath):
        for parent,dirs,files in os.walk(localPath):
            for f in files:
                sendMountQueue.put(os.path.join(parent, f))
        # print "Start to transfer mounted files, ", sendMountQueue.qsize(), "files!"
        size = sendMountQueue.qsize()
        for i in range(0,size):
            th = Process(target=searchMountPath, args=(targetPath, localPath, remoteHost, first))
            th.daemon = True
            threadpool.append(th)
            th.start()
    else:
        print ("File not found:", localPath)


def checkpointAndRestore(localWorkPath, localLogPath, remoteHost, containerid, First):
    # result is the result of threads
    result = list()

    # Get the container name
    _, container_name = getImageIDAndContainernameById(containerid)

    # make the actual path due to container name
    localWorkPath = '{0}/{1}'.format(localWorkPath, container_name)
    localLogPath = '{0}/{1}_log'.format(localLogPath, container_name)
    remote_target_path = localWorkPath

     # Get the path the container mounted
    mountPaths = getThePathContainerMounted(containerid)
    threadpool = []
    print "Start to create essential empty volumes files on remote host"
    # create file on remote host, which can done first very early
    for item in mountPaths:
        thread_transfer_mount = threading.Thread(target=sendMountfile_client, args=(item['Source'], item['Source'], remoteHost, threadpool, True))
        thread_transfer_mount.start()

    # search whether there is such image on the remote host
    # if not, make the remote host pull the image
    thread_remote_pullimage = Process(target=createMirrorContainerAndCopyFilesOnRemoteHost, args=(remoteHost, containerid, container_name))
    thread_remote_pullimage.daemon = True
    thread_remote_pullimage.start()

    if First:
        color = "\x1B[%d;%d;%dm" % (1,32,49)
        print "The container operated is",  "\x1B[%s%s\x1B[0m" % (color, container_name)

        # checkpoint the local docker container to a default dir, which is locate on tmpfs
        # thread_local_checkpoint = threading.Thread(target=localcheckpoint, args=(containerid, localWorkPath, localLogPath, True, container_name))
        # thread_local_checkpoint.start()
        # thread_local_checkpoint.join()

        # All the flags that should satisfied
        flag_local_checkpoint = False
        flag_pull_remote_image = False
        flag_transfer_file = False

        from time import time
        start = time()
        localcheckpoint(containerid, localWorkPath, localLogPath, False, container_name)
        finish = time()
        print (finish-start)

        # Now the flag_local_checkpoint is sure to be True, then to transfer the checkpoint image
        # localPath, targetPath, remoteHost are the requirements for the transfer client
        thread_transfer_files = threading.Thread(target=sendfile_client, args=(localWorkPath, remote_target_path, remoteHost))
        thread_transfer_files.start()

        for item in threadpool:
            item.join()

        # make sure checkpoint files
        thread_transfer_files.join()
        # make sure clean container created on remote host
        remoteContainerID = None
        print "Start to transfer volume data"
        # create file on remote host, which can done first very early
        for item in mountPaths:
            thread_transfer_mount = threading.Thread(target=sendMountfile_client, args=(item['Source'], item['Source'], remoteHost, threadpool, False))
            thread_transfer_mount.start()

        thread_remote_pullimage.join()

        for item in threadpool:
            item.join()

        # get pull and transfer's flag, make sure both of them is True
        while not q.empty():
            result.append(q.get())
        for item in result:
            if item[1] == createMirrorContainerAndCopyFilesOnRemoteHost.__name__:
                flag_pull_remote_image = item[0]
            if item[1] == sendfile_client.__name__:
                flag_transfer_file = item[0]
            if item[1] == "remotecontainerID":
                remoteContainerID = item[0]
        print flag_pull_remote_image, flag_transfer_file
        if flag_pull_remote_image:
            print ("Start to restore the container on {0}".format(remoteHost))
            # start a image on the remote host, and restore it from target path
            if restoreContainerOnRemoteHost(remoteHost, remoteContainerID, remote_target_path, localLogPath):
                sendChangedFilesAfterFinish(remoteHost, remoteContainerID)
            else:
                return False
        return True
    else:
        remoteurl = 'http://{0}:4433/containers/json?all=1'.format(remoteHost)
        response = requests.get(remoteurl)
        data = response.json()
        for item in data:
            names = item["Names"]
            for name in names:
                temp = name[1:]
                if temp == container_name:
                    print "start container again and restore"
                    id =  item["Id"]
                    # remote_url = 'http://{0}:4433/containers/{1}/start'.format(remoteHost,id)
                    # requests.post(remote_url)
                    restoreContainerOnRemoteHost(remoteHost, id, remote_target_path, localLogPath)
                    break


def main():
     localWorkPath, localLogPath, remoteHost, containerid = sys.argv[1:]
     if not checkpointAndRestore( localWorkPath, localLogPath, remoteHost, containerid, True):
         print "Try again, maybe network problem"
         checkpointAndRestore( localWorkPath, localLogPath, remoteHost, containerid, False)

if __name__ == "__main__":
    main()