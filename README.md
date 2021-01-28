[![Build](https://github.com/Uptycs/cloudquery/workflows/Build/badge.svg?branch=master)](https://github.com/Uptycs/cloudquery/actions?query=workflow%3ABuild)
[![Go Report Card](https://goreportcard.com/badge/github.com/Uptycs/cloudquery)](https://goreportcard.com/report/github.com/Uptycs/cloudquery)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)
---

# cloudquery powered by Osquery

cloudquery is Osquery extension to fetch cloud telemetry from AWS, GCP, and Azure. It is extensible so that  
one can add support for new tables easily, and configurable so that one can change the table schema as well.


## Contents

- [Working with Extension Binary](#build-and-test-with-extension-binary)
  * [Build](#build)
  * [Test](#test)
    + [Test with osqueryi](#with-osqueryi)  
    + [Test with osqueryd](#with-osqueryd)  

- [Working with Docker Setup](#test-with-docker)
  * [Setup](#setup-credentials)
  * [Test with osqueryi](#run-osqueryi-from-cloudquery-container)
  * [Test with osqueryd](#run-osqueryd-from-cloudquery-container)

- [Supported Tables](#supported-tables)

---

## Build and Test with Extension Binary

### Build

- Clone
- Install prerequisites
  - [go](https://golang.org/doc/install#install)
- Set environment varibale for extension home (it shoud be path-to-repo/cloudquery/extension)  
  `export CLOUDQUERY_EXT_HOME=/home/user/work/code/cloudquery/extension`
- Build extension binary.  
  `make`
- To install at default osquery directory (`/etc/osquery/`), run:  
  `make install`

### Test

#### With osqueryi

- Start osqueryi  
  `osqueryi --nodisable_extensions`
- Note down the socket path  
  `.socket`
- `cp ${CLOUDQUERY_EXT_HOME}/extension_config.json.sample ${CLOUDQUERY_EXT_HOME}/config/extension_config.json`
- Edit `${CLOUDQUERY_EXT_HOME}/config/extension_config.json` with your cloud accounts. You can add multiple accounts for each cloud provider
- Change fileName value from `"fileName": "/var/log/cloudquery.log"` to something else if you want (like `"fileName": "~/cloudquery.log"`)
- In another terminal, start extension  
  `./bin/extension --socket /path/to/socket --home-directory ${CLOUDQUERY_EXT_HOME}`
- Note that extention may fail if it cannot create the log file. So make sure that log file path exists and it can access the path. Path like `/var/log/` is accessible only to root user and hence in that case either you need to run extension as root/sudo, or change th path.
- Query data  
  `select account_id, region_code,image_id,image_type from aws_ec2_image;`

#### With osqueryd

- Build and install cloudquery
- Edit (or create if does't exist) file `/etc/osquery/extensions.load` and add the following line:
- `/etc/osquery/cloudquery.ext`
- Add following lines to `/etc/osquery/osquery.flags`  
  `--disable_extensions=false`  
  `--extensions_autoload=/etc/osquery/extensions.load`  
  `--extensions_timeout=3`  
  `--extensions_interval=3`
- Copy extension config file to `/etc/osquery/cloudquery`
  - `sudo cp ${CLOUDQUERY_EXT_HOME}/extension_config.json.sample /etc/osquery/cloudquery/config/extension_config.json`
- Edit `/etc/osquery/cloudquery/config/extension_config.json` with your cloud accounts. You can add multiple accounts for each cloud provider
  - `sudo vi /etc/osquery/cloudquery/config/extension_config.json`
- Restart osquery service.
  - `sudo service osqueryd restart`

---

## Test with Docker

### Setup Credentials

> Setup credentials before proceeding to testing with `osqueryi` or `osqueryd`

- Create a config directory on host to hold the credentials for your cloud accounts (~/config is an example, but this could be any directory):

  - `mkdir ~/config` on the machine where docker container is started
  - ~/config from the host would be mounted to /cloudquery/extension/config inside container 
- Copy `extension_config.json.sample` to your new config directory on your host:
  - Sample config file is here: [extension_config.json.sample](extension/extension_config.json.sample)
  - `cp extension/extension_config.json.sample ~/config/extension_config.json`
  -  Edit `~/config/extension_config.json` to reflect your credentials

- If using aws, copy your aws credentials:
  - `cp ~/.aws/credentials ~/config`
  - Edit credentialFile field  under aws section inside ~/config/extension_config.json and set to /cloudquery/extension/config/credentials
  - Edit id field under aws section inside ~/config/extension_config.json and set to your account id
  - Edit profileName  field under aws section inside ~/config/extension_config.json and set to your  profile name
  - Guide to create AWS credentials: https://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html

- If using Google Cloud, copy your json key file your-serviceAccount.json (cloud be any name) for your service account to `~/config`
  - `cp ~/your-serviceAccount.json ~/config`
  - Edit keyFile field under gcp section inside ~/config/extension_config.json and set to /cloudquery/extension/config/your-serviceAccount.json
  - Guide to create GCP credentials: https://cloud.google.com/iam/docs/creating-managing-service-account-keys

- If using Azure, copy the my.auth (cloud be any name) file for you account to `~/config`
  - `cp ~/my.auth ~/config`
  - Edit authFile  field under azure section inside ~/config/extension_config.json and set to /cloudquery/extension/config/my.auth
  - Edit subscriptionId and tenantId fields under azure section inside ~/config/extension_config.json and set to actual values
  - Guide to create Azure credentials: https://docs.microsoft.com/en-us/cli/azure/create-an-azure-service-principal-azure-cli?view=azure-cli-latest

### Run osqueryi from cloudquery container

`sudo docker run -it --rm -v ~/config:/cloudquery/extension/config --name cloudquery uptycs/cloudquery:latest`

Press enter to get osquery prompt

### Run osqueryd from cloudqeury container

#### Schedule queries
Identify list of scheduled queries and their intervals and place them in `osqyery.conf` inside ~/config on the host.  
Example `osquery.conf` is given below.

```json
{
  "schedule": {
    "gcp_compute_network": {
      "query": "SELECT * FROM  gcp_compute_network;",
      "interval": 120
    },
    "aws_s3_bucket": {
      "query": "SELECT * FROM aws_s3_bucket;",
      "interval": 120
    },
    "azure_compute_vm": {
      "query": "SELECT * FROM azure_compute_vm;",
      "interval": 120
    }
  }
}
```

Once all all the required files under config, run the following commands.

`mkdir ~/query-results` on your host

`sudo docker run -d --rm -v ~/config:/cloudquery/extension/config -v ~/query-results:/var/log/osquery --name cloudquery uptycs/cloudquery:latest osqueryd`

Now query results can be seen in ~/query-results

---

### Supported tables
- [AWS](extension/aws/tables.md)
- [GCP](extension/gcp/tables.md)
- [Azure](extension/azure/tables.md)
