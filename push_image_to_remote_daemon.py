import string
from time import sleep
import requests
import main

def getAllLocalImageIdsByRunningContainers():
    localurl = 'http://127.0.0.1:4433/containers/json?all=0'
    response = requests.get(localurl)
    json = response.json()
    imageids = []
    for item in json:
        imageids.append(item["ImageID"])
    return imageids

def searchImageOnRemoteHost(remotehost):
    local_image_ids = getAllLocalImageIdsByRunningContainers()
    remote_image_ids = getAllImageinfoFromHost(remotehost)
    for localid in local_image_ids:
        if localid not in remote_image_ids:
            print "start push", localid
            getLocalTarballImageAndImportToRemote(localid, remotehost)
            print "push", localid, "success"
    return False

def firstDeleteRemoteContainers(remoteHost):
    localurl = 'http://127.0.0.1:4433/containers/json'
    response = requests.get(localurl)
    for item in response.json():
        for name in item["Names"]:
            remoteurl = 'http://{0}:4433/containers/{1}?v=1&f=1'.format(remoteHost, name)
            requests.delete(remoteurl)

def checkpointMainLoop(remoteHost):
    firstDeleteRemoteContainers(remoteHost)
    localurl = 'http://127.0.0.1:4433/containers/json'
    response = requests.get(localurl)
    for item in reversed(response.json()):
        id = item["Id"]
        main.checkpointAndRestore("/home/weicheng/tmp/images","/home/weicheng/tmp/log", remoteHost, id[0:6], True)


def getAllImageinfoFromHost(host):
    localurl = 'http://{0}:4433/images/json?all=0'.format(host)
    response = requests.get(localurl)
    json = response.json()
    imageids = []
    for item in json:
        imageids.append(item["Id"])
    return imageids

def getLocalTarballImageAndImportToRemote(imageid, remoteHost):
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

if __name__ == '__main__':
    remoteHost = "192.168.2.69"
    while True:
        searchImageOnRemoteHost(remoteHost)
        checkpointMainLoop(remoteHost)
        sleep(30)