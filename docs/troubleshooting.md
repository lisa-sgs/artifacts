# Troubleshooting

## Common Issues

### S3 Access Denied

Ensure your AWS credentials have permissions for the bucket. Required permissions:

- For download: `s3:GetObject`
- For upload: `s3:PutObject`

Example IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::your-bucket/*"
    }
  ]
}
```

### Environment Variables Not Set

Make sure that `ARTIFACTS_BUCKET` is set. Others are optional.

```bash
echo $ARTIFACTS_BUCKET
```

### File Not Found

Ensure local paths exist for uploads. The library creates directories as needed for downloads.

## Logging

Enable debug logging to troubleshoot:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Support

If issues persist, open an issue on [GitHub](https://github.com/lisa-sgs/artifacts/issues).
