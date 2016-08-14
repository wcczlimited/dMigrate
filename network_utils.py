import requests


def searchNetworkOnRemote(remoteHost, networkname):
    remoteurl = 'http://%s:4433/networks?filters={"type":{"custom":true}}&filters={"name":[%s]}'% (remoteHost, networkname)
    resopnse = requests.get(remoteurl)
    if resopnse.status_code != requests.codes.ok:
        return None
    json = resopnse.json()
    for item in json:
        if item["Name"] == networkname:
            return item
    return None


def getLocalNetworkInfo(networkName):
    localurl = 'http://127.0.0.1:4433/networks/{0}'.format(networkName)
    print localurl
    response = requests.get(localurl)
    json = response.json()
    print json
    return json


def createNetworkOnRemote(remoteHost, networkName):
    rnetwork = searchNetworkOnRemote(remoteHost, networkName)
    if rnetwork is not None:
        return None
    config = getLocalNetworkInfo(networkName)
    remoteurl = 'http://{0}:4433/networks/create'.format(remoteHost)
    create_config = {
        "Name": config["Name"],
        "Driver": config["Driver"],
        "IPAM": config["IPAM"],
        "Options": config["Options"]
    }

    response = requests.post(remoteurl, json=create_config)
    if response.status_code == requests.codes.created:
        return response.json()["Id"][0:8]