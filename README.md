## SAKURA CLOUD provider for octoDNS

An [octoDNS](https://github.com/octodns/octodns/) provider that targets [SakuraCloud](https://github.com/ttkzw/octodns-sakuracloud).

### Installation

#### Command line

```
pip install git+https://github.com/ttkzw/octodns-sakuracloud.git
```

#### requirements.txt/setup.py

Pinning specific versions or SHAs is recommended to avoid unplanned upgrades.

##### Versions

```
# Start with the latest versions and don't just copy what's here
octodns==0.9.14
octodns-sakuracloud==0.0.1
```

##### SHAs

```
# Start with the latest/specific versions and don't just copy what's here
-e git+https://git@github.com/octodns/octodns.git@9da19749e28f68407a1c246dfdf65663cdc1c422#egg=octodns
-e git+https://git@github.com/octodns/octodns-sakuracloud.git@ec9661f8b335241ae4746eea467a8509205e6a30#egg=octodns_sakuracloud
```

### Configuration

```yaml
providers:
  sakuracloud:
    class: octodns_sakuracloud.SakuraCloudProvider
    # The access token for an API key
    access_token: env/SAKURACLOUD_ACCESS_TOKEN
    # The access token secret for an API key
    access_token_secret: env/SAKURACLOUD_ACCESS_TOKEN_SECRET
```

### Support Information

#### Records

SakuraCloudProvider supports A, AAAA, ALIAS, CAA, CNAME, HTTPS, MX, NS, PTR, SRV, SVCB, and TXT

#### Root NS records

SakuraCloudProvider does not supports root NS record management.

#### Dynamic

SakuraCloudProvider does not support dynamic records.

### Development

See the [/script/](/script/) directory for some tools to help with the development process. They generally follow the [Script to rule them all](https://github.com/github/scripts-to-rule-them-all) pattern. Most useful is `./script/bootstrap` which will create a venv and install both the runtime and development related requirements. It will also hook up a pre-commit hook that covers most of what's run by CI.
