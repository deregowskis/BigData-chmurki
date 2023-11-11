<h1>NiFi setup</h1>

Here's a step-by-step solution of how we setup NiFi on Google Cloud Platform (GCP).

<h3> Creating VM instance </h3>
We created a VM instance on Google Compute Engine. Equivalent code to recreate it with command line:

```
gcloud compute instances create instance-1 \
    --project=bigdata-chmurki \
    --zone=europe-central2-a \
    --machine-type=e2-medium \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=228911601895-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --tags=http-server,https-server \
    --create-disk=auto-delete=yes,boot=yes,device-name=instance-2,image=projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20231101,mode=rw,size=10,type=projects/bigdata-chmurki/zones/europe-central2-a/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --reservation-affinity=any
```

<h3>Setting up firewall rule</h3>
Equivalent REST response:

```
{
  "allowed": [
    {
      "IPProtocol": "tcp",
      "ports": [
        "8080"
      ]
    }
  ],
  "creationTimestamp": "2023-11-11T04:32:19.642-08:00",
  "description": "",
  "direction": "INGRESS",
  "disabled": false,
  "enableLogging": false,
  "id": "8619445701990075068",
  "kind": "compute#firewall",
  "logConfig": {
    "enable": false
  },
  "name": "nifiport",
  "network": "projects/bigdata-chmurki/global/networks/default",
  "priority": 1000,
  "selfLink": "projects/bigdata-chmurki/global/firewalls/nifiport",
  "sourceRanges": [
    "83.28.215.89",
    "109.243.64.116",
    "89.64.75.13",
    "194.29.137.21"
  ]
}
```

<h3>NiFi setup</h3>
After connecting SSH to the instance:

```
sudo su
apt update
apt install openjdk-8-jdk
wget https://archive.apache.org/dist/nifi/1.16.0/nifi-1.16.0-bin.tar.gz
tar -xzvf nifi-1.16.0-bin.tar.gz
cd nifi-1.16.0/conf
vim nifi.properties
```

Then correct following parameters:

```
nifi.remote.input.http.enabled = false
nifi.web.http.host=
nifi.web.http.port=8080
nifi.web.https.host=
nifi.web.https.port=
nifi.security.keystore=
nifi.security.keystoreType=
nifi.security.keystorePasswd=
nifi.security.keyPasswd=
nifi.security.truststore=
nifi.security.truststoreType=
nifi.security.truststorePasswd=
```

Then:

```
cd nifi-1.15.3/bin
./nifi.sh start
```

Finally we can run NiFi with GUI: http://34.118.106.96:8080/nifi/
