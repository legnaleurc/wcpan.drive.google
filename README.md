# wcpan.drive.google

Google Drive driver for wcpan.drive.

Please use `wcpan.drive.google.driver.GoogleDriver` as the driver class.

## Requirement

Need a `client_secret.json` file provided in **config path**.
You can download it from Google Developer Console.

## Config Example

In code:

```python
factory = DriveFactory()
# put client_secret.json at here
factory.config_path = '/path/to/config'
# assign driver class
factory.driver = 'wcpan.drive.google.driver.GoogleDriver'
factory.load_config()
```

Or in `core.yaml`:

```yaml
version: 1
database: nodes.sqlite
driver: wcpan.drive.google.driver.GoogleDriver
middleware: []
```
