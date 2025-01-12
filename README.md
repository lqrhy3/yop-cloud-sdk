# yop-cloud-sdk

This is a simple SDK for yop-cloud service.

## Installation

```bash
pip install git+https://github.com/lqrhy3/yop-cloud-sdk.git
```

## Example usage

```python
from yop_cloud_sdk import YOPStorage

storage = YOPStorage(host_url='', token='')
storage.upload(...)
```