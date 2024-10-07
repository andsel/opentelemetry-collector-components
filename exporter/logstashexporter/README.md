# Logstash Exporter

## Configuration

```yaml
exporters:
  logstash:
    enabled:
    endpoints: [ ] # hosts
    compression_level: 6 # otel http has compression. Check https://github.com/open-telemetry/opentelemetry-collector/tree/main/config/confighttp
    escape_html: false
    workers: 2
    loadbalance: true # otel grpc has balancer_name. Check https://github.com/open-telemetry/opentelemetry-collector/tree/main/config/configgrpc
    ttl: 0
    pipelining: 2
    proxy_url:
    proxy_use_local_resolver: false
    index:
    tls:
      insecure: false      # ssl.enable
      min_version: "1.2"   # ssl.supported_protocols
      max_version: ""      # ssl.supported_protocols    
      cipher_suites:       # ssl.cipher_suites
      # a single file
      ca_file:             # ssl.certificate_authorities
      # as a string instead of a filepath
      ca_pem:              # ssl.certificate_authorities
      cert_file:           # ssl.certificate
      cert_pem:            # ssl.certificate
      key_file:            # ssl.key
      key_pem:             # ssl.key
      insecure_skip_verify: # ssl.verification_mode: none
      # certificate (not support)
      # strict (not support)
      # full (default)
      reload_interval: # does agent support it?

      # Unsupported configs in otel
      # key_passphrase: # need enhancement. Contribute to upstream?
      # curve_types: # list of curve types for ECDHE (Elliptic Curve Diffie-Hellman ephemeral key exchange)
      # ca_sha256:  # base64 encoded string of the SHA-256 of the certificate
      # ca_trusted_fingerprint: # a HEX encoded SHA-256 of a CA certificate
      
      # Unsupported 
      timeout: 30s # beats has the same
      # batcher.? is experimental. Maybe not to use it
      batcher.max_size_items: # bulk_max_size
      slow_start: false
      # filebeat ignore `max_retries` and retry indefinitely
      retry_on_failure:
        enabled:
        initial_interval: 1s  # backoff.init
        max_interval: 60s     # backoff.max
        # set to 0, the retries are never stopped
        max_elapsed_time:
        multiplier:
        randomization_factor:
      sending_queue:
        enabled: false
        num_consumers:
        queue_size:
        storage:
```